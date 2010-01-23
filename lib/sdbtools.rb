require 'fattr'
require 'right_aws'
$:.unshift(File.expand_path(File.dirname(__FILE__)))
require 'sdbtools/operation'
require 'sdbtools/database'
require 'sdbtools/domain'
require 'sdbtools/selection'
require 'sdbtools/transaction'
require 'sdbtools/measured_sdb_interface'

module SDBTools

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
