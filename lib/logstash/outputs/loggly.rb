# frozen_string_literal: true

require 'logstash/outputs/base'
require 'logstash/namespace'
require 'uri'
require 'stud/buffer'
# TODO(sissel): Move to something that performs better than net/http
require 'net/http'
require 'net/https'
require 'timeout'

# Ugly monkey patch to get around <http://jira.codehaus.org/browse/JRUBY-5529>
# original link lost so the background is that in JRuby IO#read_nonblock actually
# did block, this workaround replaced an internal function that caused the blocking
# behaviourr.
# This is supposed to have been fixed in JRuby 1.7 so this probably isn't needed
# any more but we cannot say for sure without testing so it's left in for the
# refactor.
Net::BufferedIO.class_eval do
  BUFSIZE = 1024 * 16

  def rbuf_fill
    ::Timeout.timeout(@read_timeout) do
      @rbuf << @io.sysread(BUFSIZE)
    end
  end
end

# Got a loggly account? Use logstash to ship logs to Loggly!
#
# This is most useful so you can use logstash to parse and structure
# your logs and ship structured, json events to your account at Loggly.
#
# To use this, you'll need to use a Loggly input with type 'http'
# and 'json logging' enabled.
# rubocop:disable Style/ClassAndModuleChildren as from upstream
class LogStash::Outputs::Loggly < LogStash::Outputs::Base
  include Stud::Buffer

  REPORT_INTERVAL_SEC = 60
  POST_RETRY_SEC = 5
  LOGGLY_EVENT_MAX = 1024 * 1024
  LOGGLY_CHUNK_MAX = 5 * 1024 * 1024

  config_name 'loggly'
  milestone 2

  # The hostname to send logs to. This should target the loggly http input
  # server which is usually "logs.loggly.com"
  config :host, validate: :string, default: 'logs-01.loggly.com'

  # The loggly http input key to send to.
  # This is usually visible in the Loggly 'Inputs' page as something like this
  #     https://logs.hoover.loggly.net/inputs/abcdef12-3456-7890-abcd-ef0123456789
  #                                           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  #                                           \---------->   key   <-------------/
  #
  # You can use %{foo} field lookups here if you need to pull the api key from
  # the event. This is mainly aimed at multitenant hosting providers who want
  # to offer shipping a customer's logs to that customer's loggly account.
  config :key, validate: :string, required: true

  # Should the log action be sent over https instead of plain http
  config :proto, validate: :string, default: 'http'

  # Proxy Host
  config :proxy_host, validate: :string

  # Proxy Port
  config :proxy_port, validate: :number

  # Proxy Username
  config :proxy_user, validate: :string

  # Proxy Password
  config :proxy_password, validate: :password, default: ''

  # This setting controls how many events will be buffered before sending a batch
  # of events. Note that these are only batched for the same series
  config :flush_size, validate: :number, default: 100

  # The amount of time since last flush before a flush is forced.
  #
  # This setting helps ensure slow event rates don't get stuck in Logstash.
  # For example, if your `flush_size` is 100, and you have received 10 events,
  # and it has been more than `idle_flush_time` seconds since the last flush,
  # logstash will flush those 10 events automatically.
  #
  # This helps keep both fast and slow log streams moving along in
  # near-real-time.
  config :idle_flush_time, validate: :number, default: 10

  def register
    buffer_initialize(
      max_items: @flush_size,
      max_interval: @idle_flush_time,
      logger: @logger
    )
    @last_report_time = Time.now
    @events_count = 0
  end

  def receive(event)
    return unless output?(event)

    buffer_receive(event)

    handle_counts_reporting
  end

  def flush(events, _teardown)
    return if events.include?(LogStash::SHUTDOWN)

    chunks = stack_chunks(events)
    # Send the event over http.
    url = URI.parse("#{@proto}://#{@host}/bulk/#{@key}")
    @logger.info('Loggly URL', url: url)
    chunks.each do |chunk|
      Thread.new { handle_chunk(chunk, url) }
    end
  end

  private

  # The more concise this gets the more complex. This is a compromise
  # rubocop:disable Metrics/MethodLength
  # rubocop:disable Metrics/AbcSize
  # rubocop:disable Metrics/CyclomaticComplexity
  # rubocop:disable Metrics/PerceivedComplexity
  def stack_chunks(events)
    current_chunk = +''
    chunks = events.map(&:to_json).each_with_object([]) do |event, arr|
      if event.length > LOGGLY_EVENT_MAX
        @logger.warn('Event is too large to send', event: event)
        warn "#{Time.now} Event is too large to send, #{event.length}"
      elsif current_chunk.length + 1 + event.length > LOGGLY_CHUNK_MAX
        arr << current_chunk
        current_chunk = event
      else
        # Seperate chunked events
        current_chunk += "\n" unless current_chunk.empty?
        current_chunk += event
      end
    end
    # Append unbuffered data if it's there
    current_chunk.empty? && chunks || chunks << current_chunk
  end
  # rubocop:enable all

  def handle_counts_reporting
    @events_count += 1
    return unless (period = Time.now - @last_report_time) >= REPORT_INTERVAL_SEC

    @last_report_time = Time.now
    puts "#{Time.now} in last #{period} #{@events_count} events were retrieved"
    @events_count = 0
  end

  def handle_chunk(chunk, url)
    http = Net::HTTP::Proxy(
      @proxy_host, @proxy_port, @proxy_user, @proxy_password.value
    ).new(url.host, url.port)
    if url.scheme == 'https'
      http.use_ssl = true
      http.verify_mode = OpenSSL::SSL::VERIFY_NONE
    end
    post_chunk(http, url.path, chunk)
  end

  # rubocop:disable Metrics/MethodLength
  def post_chunk(http, path, chunk)
    request = Net::HTTP::Post.new(path)
    request.body = chunk
    response = http.request(request)
    if response.is_a?(Net::HTTPSuccess)
      @logger.info('Events sent to Loggly OK!')
    else
      warn "Response is not HTTPSuccess: #{response.inspect}"
      raise
    end
  rescue Exception # rubocop:disable Lint/RescueException
    warn "#{Time.now} Failed to post data to #{path}, will retry until it works"
    sleep POST_RETRY_SEC
    retry
  end
  # rubocop:enable all

  def teardown
    buffer_flush(final: true)
  end
end
