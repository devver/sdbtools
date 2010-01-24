require 'arrayfields'
require 'logger'

module SDBTools
  class Selection
    include Enumerable

    MAX_BATCH_LIMIT      = 250
    DEFAULT_RESULT_LIMIT = 100

    attr_accessor :domain
    attr_accessor :attributes
    attr_accessor :conditions
    attr_reader   :limit
    attr_reader   :batch_limit
    attr_accessor :offset
    attr_accessor :order_by
    attr_accessor :order
    attr_accessor :sdb
    attr_accessor :logger

    # For testing
    attr_writer :starting_token

    def self.quote_name(name)
      if name.to_s =~ /^[A-Z$_][A-Z0-9$_]*$/i
        name.to_s
      else
        "`" + name.to_s.gsub("`", "``") + "`"
      end
    end

    def self.quote_value(value)
      '"' + value.to_s.gsub(/"/, '""') + '"'
    end

    def initialize(sdb, domain, options={})
      @sdb             = sdb
      @domain          = domain.to_s
      @attributes      = options.fetch(:attributes) { :all }
      @conditions      = options[:conditions].to_s
      @order           = options.fetch(:order) { :ascending }
      @order_by        = options.fetch(:order_by) { :none }
      @order_by        = @order_by.to_s unless @order_by == :none
      self.limit       = options.fetch(:limit) { :none }
      self.batch_limit = options.fetch(:batch_limit) { DEFAULT_RESULT_LIMIT }.to_i
      @offset          = options.fetch(:offset) { 0 }.to_i
      @logger          = options.fetch(:logger){::Logger.new($stderr)}
    end

    def to_s(query_limit=limit, offset=0)
      "SELECT #{output_list}"       \
      " FROM #{quote_name(domain)}" \
      "#{match_expression}"         \
      "#{sort_instructions}"        \
      "#{limit_clause(query_limit,offset)}"
    end

    def count_expression
      "SELECT count(*) FROM #{quote_name(domain)}#{match_expression}#{sort_instructions}#{limit_clause_for_count}"
    end

    def offset_count_expression
      "SELECT count(*) FROM #{quote_name(domain)}#{match_expression}#{sort_instructions} LIMIT #{offset}"
    end

    def count
      Transaction.open(count_expression) do |t|
        @count ||= count_operation.inject(0){|count, (results, operation)| 
          count += results[:items].first["Domain"]["Count"].first.to_i
        }
      end
    end

    alias_method :size, :count
    alias_method :length, :count

    def each
      return if limit == 0
      Transaction.open(to_s) do
        num_items = 0
        select_operation(limit, num_items).each do |results, operation|
          results[:items].each do |item|
            yield(item.keys.first, item.values.first)
            num_items += 1
            return if limit != :none && num_items >= limit
          end
          operation.args[0] = to_s(limit, num_items)
        end
      end
    end

    def results
      @results ||= inject(Arrayfields.new){|results, (name, value)|
        results[name] = value
        results
      }
    end

    def count_operation
      Operation.new(sdb, :select, count_expression, :starting_token => starting_token)
    end

    def offset_count_operation
      Operation.new(sdb, :select, offset_count_expression)
    end

    def select_operation(query_limit=limit, offset=0)
      Operation.new(sdb, :select, to_s(query_limit, offset), :starting_token => starting_token)
    end

    def starting_token
      @starting_token ||=
        case offset
        when 0 then nil
        else
          op    = offset_count_operation
          count = 0
          op.each do |results, operation|
          count += results[:items].first["Domain"]["Count"].first.to_i
          if count == offset || results[:next_token].nil?
            return results[:next_token]
          end
        end
          raise "Failed to find offset #{offset}"
        end
    end

    def limit=(new_limit)
      case new_limit
      when :none, Integer then 
        @limit = new_limit
      else
        raise ArgumentError, "Limit must be integer or :none"
      end
    end

    def batch_limit=(new_limit)
      case new_limit
      when (1..MAX_BATCH_LIMIT) then 
        @batch_limit = new_limit
      else
        raise RangeError, "Limit must be 1..#{MAX_BATCH_LIMIT}"
      end
    end

    private

    def quote_name(name)
      self.class.quote_name(name)
    end

    def quote_value(value)
      self.class.quote_value(value)
    end

    def output_list
      case attributes
      when Array then attributes.map{|a| quote_name(a)}.join(", ")
      when :all then "*"
      else raise ScriptError, "Bad attributes: #{attributes.inspect}"
      end
    end

    def match_expression
      if conditions.empty? then "" else " WHERE #{conditions}" end
    end

    def limit_clause(query_limit=:none, offset=0)
      case query_limit
      when :none then format_limit_clause(batch_limit)
      else
        remaining_query_limit = query_limit - offset
        this_batch_limit = 
          remaining_query_limit < batch_limit ? remaining_query_limit : batch_limit
        format_limit_clause(this_batch_limit)
      end
    end

    def format_limit_clause(this_batch_limit)
      case this_batch_limit
      when DEFAULT_RESULT_LIMIT then ""
      else " LIMIT #{this_batch_limit}"
      end
    end

    # There are special rules for the LIMIT clause when executing a count(*)
    # select. Normally it specifies batch size, capped at 250. But for count(*)
    # expressions it specifies the max count, which may be arbitrarily large
    def limit_clause_for_count
      case limit
      when :none then ""
      else " LIMIT #{limit}"
      end
    end

    def sort_instructions
      case order_by
      when :none then ""
      else 
        direction = (order == :ascending ? "ASC" : "DESC")
        " ORDER BY #{quote_name(order_by)} #{direction}"
      end
    end
  end
end
