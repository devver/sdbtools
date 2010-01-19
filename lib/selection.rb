require 'arrayfields'

module SDBTools
  class Selection
    include Enumerable

    MAX_RESULT_LIMIT     = 250
    DEFAULT_RESULT_LIMIT = 100

    attr_accessor :domain
    attr_accessor :attributes
    attr_accessor :conditions
    attr_accessor :limit
    attr_accessor :offset
    attr_accessor :sdb

    # For testing
    attr_writer :starting_token

    def initialize(sdb, domain, options={})
      @sdb        = sdb
      @domain     = domain.to_s
      @attributes = options.fetch(:attributes) { :all }
      @conditions = Array(options[:conditions])
      @limit      = options.fetch(:limit) { DEFAULT_RESULT_LIMIT }.to_i
      @offset     = options.fetch(:offset) { 0 }.to_i
    end

    def to_s
      "SELECT #{output_list} FROM #{quote_name(domain)}#{match_expression}#{limit_clause}"
    end

    def count
      @count ||= count_operation.inject(0){|count, results| 
        count += results[:items].first["Domain"]["Count"].first.to_i
      }
    end

    alias_method :size, :count
    alias_method :length, :count

    def each
      select_operation.each do |results|
        results[:items].each do |item|
          yield(item.keys.first, item.values.first)
        end
      end
    end

    def results
      @results ||= inject(Arrayfields.new){|results, (name, value)|
        results[name] = value
        results
      }
    end

    def count_expression
      "SELECT count(*) FROM #{quote_name(domain)}#{match_expression}"
    end

    def offset_count_expression
      "#{count_expression} LIMIT #{offset}"
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

    private

    def quote_name(name)
      if name.to_s =~ /^[A-Z$_][A-Z0-9$_]*$/i
        name.to_s
      else
        "`" + name.to_s.gsub("`", "``") + "`"
      end
    end

    def quote_value(value)
      '"' + value.to_s.gsub(/"/, '""') + '"'
    end

    def output_list
      case attributes
      when Array then attributes.map{|a| quote_name(a)}.join(", ")
      when :all then "*"
      else raise ScriptError, "Bad attributes: #{attributes.inspect}"
      end
    end

    def match_expression
      case conditions.size
      when 0 then ""
      else " WHERE #{prepared_conditions.join(' ')}"
      end
    end

    def prepared_conditions
      conditions.map { |condition|
        case condition
        when String then condition
        when Array then
          values   = condition.dup
          template = values.shift.to_s
          template.gsub(/\?/) {|match|
            quote_value(values.shift.to_s)
          }
        else
          raise ScriptError, "Bad condition: #{condition.inspect}"
        end
      }
    end

    def limit_clause
      case limit
      when DEFAULT_RESULT_LIMIT then ""
      else " LIMIT #{limit}"
      end
    end
  end
end
