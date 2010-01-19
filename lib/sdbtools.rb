require 'fattr'
require 'right_aws'
require File.expand_path('selection', File.dirname(__FILE__))

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

  class Database
    def initialize(access_key=nil, secret_key=nil, options={})
      @sdb = RightAws::SdbInterface.new(access_key, secret_key, options)
      @logger = options.fetch(:logger){::Logger.new($stderr)}
    end

    def domains
      domains_op = Operation.new(@sdb, :list_domains, nil)
      domains_op.inject([]) {|domains, results|
        domains.concat(results[:domains])
        domains
      }
    end

    def domain(domain_name)
      Domain.new(@sdb, domain_name)
    end

    def domain_exists?(domain_name)
      domains.include?(domain_name)
    end

    def create_domain(domain_name)
      @sdb.create_domain(domain_name)
      domain(domain_name)
    end

    def delete_domain(domain_name)
      @sdb.delete_domain(domain_name)
    end

    def make_dump(domain, filename)
      Dump.new(domain, filename, @logger)
    end

    def make_load(domain, filename)
      Load.new(domain, filename, @logger)
    end

    private
  end

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
      op = Operation.new(@sdb, :select, "select count(*) from #{name}")
      @count = op.inject(0) {|count, results|
        count += results[:items].first["Domain"]["Count"].first.to_i
        count
      }
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

    def put(item_name, attributes)
      @sdb.put_attributes(@name, item_name, attributes)
    end

    def select(query)
      op = Operation.new(@sdb, :select, "select * from #{name} where #{query}")
      op.inject([]){|items,results|
        batch_items = results[:items].map{|pair|
          item = pair.values.first
          item.merge!({'itemName()' => pair.keys.first})
          item
        }
        items.concat(batch_items)
      }
    end

    def delete(item_name)
      @sdb.delete_attributes(@name, item_name)
    end
  end

  class Task
    fattr :callback    => lambda{}
    fattr :chunk_size  => 100

    def initialize(status_file, logger)
      @logger = logger
      @status_file = Pathname(status_file)
      @status = PStore.new(@status_file.to_s)
      unless @status_file.exist?
        initialize_status!(yield)
      end
      @status.transaction(false) do
        @logger.info "Initializing #{@status_file} for PID #{$$}"
        @status[$$] = {}
        @status[$$][:working_items] = []
      end
    end

    def session
      yield
    ensure
      release_working_items!
    end

    def incomplete_count
      @status.transaction(true) {|status|
        Array(status[:incomplete_items]).size
      }
    end

    def failed_items
      @status.transaction(true) do |status|
        status[:failed_items]
      end
    end

    def reserve_items!(size)
      @status.transaction(false) do |status|
        chunk = status[:incomplete_items].slice!(0,size)
        status[$$][:working_items].concat(chunk)
        @logger.info("Reserved #{chunk.size} items")
        chunk
      end
    end

    def finish_items!(items)
      @status.transaction(false) do |status|
        items.each do |item_name|
          status[:complete_items] << 
            status[$$][:working_items].delete(item_name)
          @logger.info("Marked item #{item_name} complete")
        end
      end
    end

    def release_working_items!
      @logger.info("Releasing working items")
      @status.transaction(false) do |status|
        items = status[$$][:working_items]
        status[:incomplete_items].concat(items)
        status[$$][:working_items].clear
      end
    end

    def report
      @status.transaction(true) do |status|
        done     = status[:complete_items].size
        not_done = status[:incomplete_items].size
        failed   = status[:failed_items].size
        puts "Items (not done/done/failed): #{not_done}/#{done}/#{failed}"
        status.roots.select{|root| root.kind_of?(Integer)}.each do |root|
          pid = root
          items = status[root][:working_items].size
          puts "Process #{pid} working on #{items} items"
        end
      end
    end

    private

    def record_failed_item!(item_name, info)
      @logger.info "Problem with item #{item_name}"
      @status.transaction(false) do |status|
        status[:failed_items] ||= {}
        status[:failed_items][item_name] = info
      end
    end
    
    def initialize_status!(item_names)
      @status.transaction(false) do |status|
        status[:incomplete_items] = item_names
        status[:failed_items]     = {}
        status[:complete_items]   = []
      end
    end
  end

  class Dump < Task
    def initialize(domain, filename, logger)
      @domain                   = domain
      @dump_filename            = Pathname(filename)
      super(status_filename, logger) {
        @domain.item_names
      }
    end

    def status_filename
      Pathname(
        @dump_filename.basename(@dump_filename.extname).to_s + 
        ".simpledb_op_status")
    end

    def start!
      session do
        until (chunk = reserve_items!(chunk_size)).empty?
          items = @domain.items(chunk)
          dump_items(items)
          finish_items!(chunk)
          items.each do |item| callback.call(item) end
        end
      end
    end

    private

    def dump_items(items)
      @logger.info "Dumping #{items.size} items to #{@dump_filename}"
      FileUtils.touch(@dump_filename) unless @dump_filename.exist?
      file = File.new(@dump_filename.to_s)
      file.flock(File::LOCK_EX)
      @dump_filename.open('a+') do |f|
        items.each_pair do |item_name, attributes|
          @logger.info "Dumping item #{item_name}"
          YAML.dump({item_name => attributes}, f)
        end
      end
    ensure
        file.flock(File::LOCK_UN)
    end

  end

  class Load < Task
    def initialize(domain, filename, logger)
      @domain    = domain
      @dump_filename = Pathname(filename)
      @dump_file = DumpFile.new(filename)
      super(status_filename, logger) {
        @dump_file.item_names
      }
    end

    def status_filename
      Pathname(
        @dump_filename.basename(@dump_filename.extname).to_s + 
        "-load-#{@domain.name}.simpledb_op_status")
    end

    def start!
      session do
        chunk    = []
        reserved = Set.new
        @dump_file.each do |item_name, attributes|
          if reserved.empty?
            finish_items!(chunk)
            reserved.replace(chunk = reserve_items!(chunk_size))
            break if chunk.empty?
          end
          if reserved.include?(item_name)
            @logger.info("#{item_name} is reserved, loading to #{@domain.name}")
            begin
              @domain.put(item_name, attributes)
            rescue
              record_failed_item!(
                item_name, 
                "#{$!.class.name}: #{$!.message}")
            end
            reserved.delete(item_name)
            callback.call(item_name)
          end
        end
        finish_items!(chunk)
      end
    end
  end

  class DumpFile
    include Enumerable

    def initialize(path)
      @path = Pathname(path)
    end

    def item_names
      map{|item_name, item| item_name}
    end

    def size
      inject(0){|size, item_name, item|
        size += 1
      }
    end

    def each
      @path.open('r') {|f|
        YAML.load_documents(f) {|doc|
          yield doc.keys.first, doc.values.first
        }
      }
    end
  end

end
