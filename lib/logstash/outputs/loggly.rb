# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "uri"
require "stud/buffer"
# TODO(sissel): Move to something that performs better than net/http
require "net/http"
require "net/https"


# Ugly monkey patch to get around <http://jira.codehaus.org/browse/JRUBY-5529>
Net::BufferedIO.class_eval do
    BUFSIZE = 1024 * 16

    def rbuf_fill
      timeout(@read_timeout) {
        @rbuf << @io.sysread(BUFSIZE)
      }
    end
end

# Got a loggly account? Use logstash to ship logs to Loggly!
#
# This is most useful so you can use logstash to parse and structure
# your logs and ship structured, json events to your account at Loggly.
#
# To use this, you'll need to use a Loggly input with type 'http'
# and 'json logging' enabled.
class LogStash::Outputs::Loggly < LogStash::Outputs::Base
  include Stud::Buffer
  config_name "loggly"
  milestone 2

  # The hostname to send logs to. This should target the loggly http input
  # server which is usually "logs.loggly.com"
  config :host, :validate => :string, :default => "logs-01.loggly.com"

  # The loggly http input key to send to.
  # This is usually visible in the Loggly 'Inputs' page as something like this
  #     https://logs.hoover.loggly.net/inputs/abcdef12-3456-7890-abcd-ef0123456789
  #                                           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  #                                           \---------->   key   <-------------/
  #
  # You can use %{foo} field lookups here if you need to pull the api key from
  # the event. This is mainly aimed at multitenant hosting providers who want
  # to offer shipping a customer's logs to that customer's loggly account.
  config :key, :validate => :string, :required => true

  # Should the log action be sent over https instead of plain http
  config :proto, :validate => :string, :default => "http"

  # Proxy Host
  config :proxy_host, :validate => :string

  # Proxy Port
  config :proxy_port, :validate => :number

  # Proxy Username
  config :proxy_user, :validate => :string

  # Proxy Password
  config :proxy_password, :validate => :password, :default => ""

  # This setting controls how many events will be buffered before sending a batch
  # of events. Note that these are only batched for the same series
  config :flush_size, :validate => :number, :default => 100

  # The amount of time since last flush before a flush is forced.
  #
  # This setting helps ensure slow event rates don't get stuck in Logstash.
  # For example, if your `flush_size` is 100, and you have received 10 events,
  # and it has been more than `idle_flush_time` seconds since the last flush,
  # logstash will flush those 10 events automatically.
  #
  # This helps keep both fast and slow log streams moving along in
  # near-real-time.
  config :idle_flush_time, :validate => :number, :default => 10

  LOGGLY_EVENT_MAX_SIZE = 1024 * 1024
  LOGGLY_CHUNK_MAX_SIZE = 5 * 1024 * 1024


  public
  def register
    buffer_initialize(
      :max_items => @flush_size,
      :max_interval => @idle_flush_time,
      :logger => @logger
    )
    @last_count_report = Time.now
    @events_count = 0
  end

  public
  def receive(event)
    return unless output?(event)
    buffer_receive(event)

    handle_counts_reporting
  end

  private
  def handle_counts_reporting
    @events_count+=1

    secs_since_last_report = Time.now - @last_count_report

    if secs_since_last_report >= 60
      @last_count_report = Time.now
      puts "#{Time.now} in last #{secs_since_last_report} #{@events_count} events were retrieved"
      @events_count = 0
    end
  end

  public
  def flush(events, teardown=false)
    current_chunk = ""
    chunks = []

    events.each do |event|
      if event == LogStash::SHUTDOWN
        finished
        return
      end

      event = event.to_json

      if event.length > LOGGLY_EVENT_MAX_SIZE
        @logger.warn("Event is too large to send", :event => event)
        puts "#{Time.now} Event is too large to send, #{event.length}"
      elsif (current_chunk.length + 1 + event.length) > LOGGLY_CHUNK_MAX_SIZE
        chunks << current_chunk
        current_chunk = event
      else
        current_chunk << "\n" if current_chunk.length > 0
        current_chunk << event
      end
    end

    chunks << current_chunk if current_chunk.length > 0

    # Send the event over http.
    url = URI.parse("#{@proto}://#{@host}/bulk/#{@key}")
    @logger.info("Loggly URL", :url => url)


    chunks.each do |chunk|
      handle_chunk(chunk, url)
    end

  end

  private
  def handle_chunk(chunk, url)
    Thread.new(chunk, url) {|_chunk, _url|
      retries = 5

      http = Net::HTTP::Proxy(@proxy_host, @proxy_port, @proxy_user, @proxy_password.value).new(_url.host, _url.port)
      if _url.scheme == 'https'
        http.use_ssl = true
        http.verify_mode = OpenSSL::SSL::VERIFY_NONE
      end

      begin
        request = Net::HTTP::Post.new(_url.path)
        request.body = _chunk
        response = http.request(request)
        if response.is_a?(Net::HTTPSuccess)
          @logger.info("Events sent to Loggly OK!")
        else
          puts "Response is not HTTPSuccess"
          @logger.warn("HTTP error", :error => response.error!)
        end
      rescue Exception
        retries -= 1
        puts "#{Time.now} Failed to post data to #{_url.path}, #{retries} retries left, #{$!}"
        sleep 5
        retry if retries > 0
      end
    }
  end

  def teardown
    buffer_flush(:final => true)
  end # def teardown
end # class LogStash::Outputs::Loggly