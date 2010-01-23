require 'arrayfields'
require 'logger'

module SDBTools
  class Selection
    include Enumerable

    MAX_RESULT_LIMIT     = 250
    DEFAULT_RESULT_LIMIT = 100

    attr_accessor :domain
    attr_accessor :attributes
    attr_accessor :conditions
    attr_reader   :limit
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
      @sdb        = sdb
      @domain     = domain.to_s
      @attributes = options.fetch(:attributes) { :all }
      @conditions = options[:conditions].to_s
      @order      = options.fetch(:order) { :ascending }
      @order_by   = options.fetch(:order_by) { :none }
      @order_by   = @order_by.to_s unless @order_by == :none
      self.limit  = options.fetch(:limit) { DEFAULT_RESULT_LIMIT }
      @offset     = options.fetch(:offset) { 0 }.to_i
      @logger     = options.fetch(:logger){::Logger.new($stderr)}
    end

    def to_s
      "SELECT #{output_list} FROM #{quote_name(domain)}#{match_expression}#{sort_instructions}#{limit_clause}"
    end

    def count_expression
      "SELECT count(*) FROM #{quote_name(domain)}#{match_expression}#{sort_instructions}#{limit_clause_for_count}"
    end

    def offset_count_expression
      "SELECT count(*) FROM #{quote_name(domain)}#{match_expression}#{sort_instructions} LIMIT #{offset}"
    end

    def count
      Transaction.open(count_expression) do |t|
        @count ||= count_operation.inject(0){|count, results| 
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
        select_operation.each do |results|
          results[:items].each do |item|
            yield(item.keys.first, item.values.first)
            num_items += 1
            return if limit != :none && num_items >= limit
          end
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

    def select_operation
      Operation.new(sdb, :select, to_s, :starting_token => starting_token)
    end

    def starting_token
      @starting_token ||=
        case offset
        when 0 then nil
        else
          op    = offset_count_operation
          count = 0
          op.each do |results|
          count += results[:items].first["Domain"]["Count"].first.to_i
          if count == offset || results[:next_token].nil?
            return results[:next_token]
          end
        end
          raise "Failed to find offset #{offset}"
        end
    end

    def limit=(new_limit)
      # We can't yet support large limits. In order to do so, it will be necessary
      # to implement limit chunking, where the limit is split across multiple
      # requests of 250 items and a final request of limit % 250 items.
      case new_limit
      when :none, (0..MAX_RESULT_LIMIT) then 
        @limit = new_limit
      else
        raise RangeError, "Limit must be 0..250 or :none"
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

    def limit_clause
      case limit
      when :none then " LIMIT #{MAX_RESULT_LIMIT}"
      when DEFAULT_RESULT_LIMIT then ""
      else 
        batch_limit = limit < MAX_RESULT_LIMIT ? limit : MAX_RESULT_LIMIT
        " LIMIT #{batch_limit}"
      end
    end

    def limit_clause_for_count
      case limit
      when :none then ""
      else limit_clause
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
