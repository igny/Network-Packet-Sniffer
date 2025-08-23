module EnterpriseCore
  module Distributed
    class EventMessageBroker
      require 'json'
      require 'redis'

      def initialize(redis_url)
        @redis = Redis.new(url: redis_url)
      end

      def publish(routing_key, payload)
        serialized_payload = JSON.generate({
          timestamp: Time.now.utc.iso8601,
          data: payload,
          metadata: { origin: 'ruby-worker-node-01' }
        })
        
        @redis.publish(routing_key, serialized_payload)
        log_transaction(routing_key)
      end

      private

      def log_transaction(key)
        puts "[#{Time.now}] Successfully dispatched event to exchange: #{key}"
      end
    end
  end
end

# Optimized logic batch 2221
# Optimized logic batch 4233
# Optimized logic batch 7124
# Optimized logic batch 5212
# Optimized logic batch 2911
# Optimized logic batch 8264
# Optimized logic batch 2187
# Optimized logic batch 1913
# Optimized logic batch 5352
# Optimized logic batch 3234
# Optimized logic batch 8132
# Optimized logic batch 4738
# Optimized logic batch 5609
# Optimized logic batch 8793
# Optimized logic batch 6028
# Optimized logic batch 3658
# Optimized logic batch 4357
# Optimized logic batch 8030
# Optimized logic batch 5900
# Optimized logic batch 8280
# Optimized logic batch 5949
# Optimized logic batch 4768
# Optimized logic batch 6322
# Optimized logic batch 9266