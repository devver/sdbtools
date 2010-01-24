require 'sdbtools/transaction'
require 'delegate'

module SDBTools
  class MeasuredSdbInterface < DelegateClass(RightAws::SdbInterface)
    def create_domain(*args, &block)
      Transaction.open("create_domain") do |t|
        t.measure_aws_call{super(*args, &block)}
      end
    end
    
    def delete_domain(*args, &block)
      Transaction.open("delete_domain") do |t|
        t.measure_aws_call{super(*args, &block)}
      end
    end

    def list_domains(*args, &block)
      Transaction.open("list_domains") do |t|
        t.measure_aws_call{super(*args, &block)}
      end
    end

    def put_attributes(*args, &block)
      Transaction.open("put_attributes") do |t|
        t.measure_aws_call{super(*args, &block)}
      end
    end

    def get_attributes(*args, &block)
      Transaction.open("get_attributes") do |t|
        t.measure_aws_call{super(*args, &block)}
      end
    end

    def select(*args, &block)
      Transaction.open("select #{args.first}") do |t|
        t.measure_aws_call{super(*args, &block)}
      end
    end

    def query(*args, &block)
      Transaction.open("query") do |t|
        t.measure_aws_call{super(*args, &block)}
      end
    end
  end
end
