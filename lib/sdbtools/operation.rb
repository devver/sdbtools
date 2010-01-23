module SDBTools
  # An Operation represents a SimpleDB operation and handles the details of
  # re-requesting "next tokens" until the operation is complete.
  class Operation
    include Enumerable

    attr_reader :method
    attr_reader :args
    attr_reader :starting_token

    def initialize(sdb, method, *args)
      @options = args.last.is_a?(Hash) ? args.pop : {}
      @sdb     = sdb
      @method  = method
      @args    = args
      @starting_token = @options[:starting_token]
    end

    # Yields once for each result set, until there is no next token.
    def each
      Transaction.open(":#{method} operation") do |t|
        next_token = starting_token
        begin
          args = @args.dup
          args << next_token
          results = @sdb.send(@method, *args)
          yield(results)
          next_token = results[:next_token]
        end while next_token
      end
    end

  end
end
