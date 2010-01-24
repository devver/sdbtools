module SDBTools
  class Database
    attr_reader   :sdb
    attr_accessor :logger

    def initialize(access_key=nil, secret_key=nil, options={})
      @logger = (options[:logger] ||= ::Logger.new($stderr))
      @sdb    =         MeasuredSdbInterface.new(
        options.delete(:sdb_interface) {
          RightAws::SdbInterface.new(access_key, secret_key, options)
        })
    end

    def domains
      domains_op = Operation.new(@sdb, :list_domains, nil)
      domains_op.inject([]) {|domains, (results, operation)|
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
end
