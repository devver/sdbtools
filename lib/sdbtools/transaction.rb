require 'benchmark'

module SDBTools

  # SimpleDB is not remotely transactional.  A Transaction in this context is
  # just a way to group together a series of SimpleDB requests for the purpose
  # of benchmarking.
  class Transaction

    def self.open(description, on_close=self.on_close)
      transaction = self.new(description, on_close)
      transaction_stack.push(transaction)
      yield(transaction)
    ensure
      transaction = transaction_stack.pop
      transaction.close
    end

    # Set the default on_close action. An on_close action receives a Transaction
    # object which is being closed
    def self.on_close=(action)
      @on_close_action = action
    end

    # Get the default on_close action
    def self.on_close
      if current
        current.on_close
      else
        @on_close_action ||= lambda{|t|}
      end
    end

    # Usage:
    #   Transaction.on_close = Transaction.log_transaction_close(logger)
    def self.log_transaction_close(logger, cutoff_level=:none)
      pattern = "%s \"%s\" (User %0.6u; System %0.6y; CPU %0.6t; Clock %0.6r; Box %0.6f; Reqs %d; Items %d)"
      lambda do |t|
        if cutoff_level == :none || cutoff_level <= t.nesting_level
          prefix = "*" * (t.nesting_level + 1)
          logger.info(
            t.times.format(
              pattern,
              prefix, 
              t.description,
              t.box_usage, 
              t.request_count, 
              t.item_count))
        end
      end
    end

    def self.add_stats(box_usage, request_count, item_count, times)
      current and current.add_stats(box_usage, request_count, item_count, times)
    end

    def self.current
      transaction_stack.last
    end

    def self.transaction_stack
      Thread.current[:sdbtools_transaction_stack] ||= []
    end

    attr_reader :description
    attr_reader :box_usage
    attr_reader :request_count
    attr_reader :item_count
    attr_reader :times
    attr_reader :on_close
    attr_reader :nesting_level

    def initialize(description, on_close_action=self.class.on_close)
      @description   = description
      @box_usage     = 0.0
      @request_count = 0
      @item_count    = 0
      @on_close      = on_close_action
      @times         = Benchmark::Tms.new
      @nesting_level = self.class.transaction_stack.size
    end

    def close
      self.class.add_stats(@box_usage, @request_count, @item_count, @times)
      @on_close.call(self)
      self
    end

    def add_stats(box_usage, request_count, item_count, times=Benchmark::Tms.new)
      @request_count += request_count.to_i
      @box_usage     += box_usage.to_f
      @item_count    += item_count
      @times         += times
    end

    def add_stats_from_aws_response(response)
      response   = response.nil? ? {} : response
      box_usage  = response[:box_usage].to_f
      item_count = Array(response[:items]).size + Array(response[:domains]).size
      item_count += 1 if response.key?(:attributes)
      add_stats(box_usage, 1, item_count)
      response
    end

    def measure_aws_call
      add_stats_from_aws_response(time{yield})
    end

    # Benchmark the block and add its times to the transaction
    def time
      result = nil
      self.times.add! do 
        result = yield 
      end
      result
    end

  end
end
