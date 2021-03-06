module SDBTools
  class Domain
    attr_reader :name

    def initialize(sdb, name)
      @sdb        = sdb
      @name       = name
      @item_names = nil
      @count      = nil
    end

    def [](item_name)
      @sdb.get_attributes(name, item_name)[:attributes]
    end

    def item_names
      return @item_names if @item_names
      query = Operation.new(@sdb, :query, @name, nil, nil)
      @item_names = query.inject([]) {|names, results| 
        names.concat(results[:items])
        names
      }
    end

    def count
      return @count if @count
      @count = selection.count
    end

    def items(item_names)
      names  = item_names.map{|n| "'#{n}'"}.join(', ')
      query  = "select * from #{name} where itemName() in (#{names})"
      select = Operation.new(@sdb, :select, query)
      select.inject({}) {|items, results|
        results[:items].each do |item|
          item_name  = item.keys.first
          item_value = item.values.first
          items[item_name] = item_value
        end
        items
      }
    end

    def get(item_name, attribute_name=nil)
      @sdb.get_attributes(@name, item_name, attribute_name)
    end

    def put(item_name, attributes, options={})
      replace = options[:replace] ? :replace : false
      @sdb.put_attributes(@name, item_name, attributes, replace)
    end

    def selection(options={})
      Selection.new(@sdb, name, options)
    end

    # Somewhat deprecated. Use #selection() instead
    def select(query)
      op = Operation.new(@sdb, :select, "select * from #{name} where #{query}")
      op.inject([]){|items,(results, operation)|
        batch_items = results[:items].map{|pair|
          item = pair.values.first
          item.merge!({'itemName()' => pair.keys.first})
          item
        }
        items.concat(batch_items)
      }
    end

    def delete(item_name, attributes={})
      @sdb.delete_attributes(@name, item_name, attributes)
    end
  end
end
