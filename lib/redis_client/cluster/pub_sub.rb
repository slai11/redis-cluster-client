# frozen_string_literal: true

class RedisClient
  class Cluster
    class PubSub
      def initialize(router, command_builder)
        @router = router
        @command_builder = command_builder

        @pubsub_map = {}
        @thread_map = {}

        @mutex = Mutex.new
        @result_buffer = []

        @result_monitor = Monitor.new
        @result_condition = @result_monitor.new_cond
      end

      def call(*args, **kwargs)
        command = @command_builder.generate(args, kwargs)
        node = @router.assign_node(command)
        node_id = node.inspect
        @pubsub_map[node_id] = node.pubsub if @pubsub_map[node_id].nil?
        @pubsub_map[node_id].call_v(command)
      end

      def call_v(command)
        command = @command_builder.generate(command)
        node = @router.assign_node(command)
        node_id = node.inspect
        @pubsub_map[node_id] = node.pubsub if @pubsub_map[node_id].nil?
        @pubsub_map[node_id].call_v(command)
      end

      def close
        @thread_map.values.map(&:kill)
        @pubsub_map.values.map(&:close)

        @result_buffer = []
        @thread_map = {}
        @pubsub_map = {}
      end

      def next_event(timeout = nil)
        return nil if @pubsub_map.empty?

        # setup new bg threads if needed for new nodes
        @pubsub_map.each do |key, pubsub|
          next if @thread_map[key] && @thread_map[key].alive?

          @thread_map[key] = Thread.new do
            # use timeout so threads last as long as timeout variable
            event = pubsub&.next_event(timeout)
            if !event.nil?
              @mutex.synchronize { @result_buffer << event }
              @result_monitor.synchronize { @result_condition.signal }
            end
          end
        end

        if @result_buffer.empty?
          @result_monitor.synchronize do
            wait_timeout = timeout == 0 ? nil : timeout
            @result_condition.wait(wait_timeout)
          end
        end

        @result_buffer.pop
      end
    end
  end
end
