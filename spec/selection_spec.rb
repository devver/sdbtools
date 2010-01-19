require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

module SDBTools
  describe Selection do
    def count_results(count, token=nil)
      {
        :items => [
          "Domain" => {
            "Count" => [count.to_s]
          }
        ],
        :next_token => token
      }      
    end

    def select_results(names, token=nil)
      {
        :items => names.map{|n| {n => {"#{n}_attr" => ["#{n}_val"]}}},
        :next_token => token
      }      
    end

    before :each do
      @sdb = stub("SDB")
    end

    context "with domain THE_DOMAIN" do
      before :each do
        @it = Selection.new(@sdb, "THE_DOMAIN")
      end

      specify { @it.to_s.should == "SELECT * FROM THE_DOMAIN" }

      it "should be able to generate a count expression" do
        @it.count_expression.should be == "SELECT count(*) FROM THE_DOMAIN"
      end

      it "should be able to generate a count operation" do
        op = @it.count_operation
        op.method.should be == :select
        op.args.should be == ["SELECT count(*) FROM THE_DOMAIN"]
        op.starting_token.should be_nil
      end

      it "should generate a nil start token" do
        @it.starting_token.should be_nil
      end
    end

    context "with domain 1DO`MAIN" do
      before :each do
        @it = Selection.new(@sdb, "1DO`MAIN")
      end

      specify { @it.to_s.should == "SELECT * FROM `1DO``MAIN`" }
    end

    context "with explicit attributes" do
      before :each do
        @it = Selection.new(@sdb, "DOMAIN", :attributes => [:foo, :bar])
      end

      it "should select the attributes" do
        @it.to_s.should == "SELECT foo, bar FROM DOMAIN"
      end
    end

    context "with funky attribute names" do
      before :each do
        @it = Selection.new(@sdb, "DOMAIN", 
          :attributes => ["attr foo", "1`bar", "baz"])
      end

      it "should quote the funky attribute names" do
        @it.to_s.should == %Q{SELECT `attr foo`, `1``bar`, baz FROM DOMAIN}
      end
    end

    context "with conditions" do
      before :each do
        @it = Selection.new(@sdb, "DOMAIN", 
          :attributes => ["attr foo", "bar"],
          :conditions => [
            'bar == "buz"', 
            "AND",
            ['`attr foo` between ? and ?', 4, '1"2']])
      end

      it "should format quote and interpolate the conditions" do
        @it.to_s.should be == 
          %Q{SELECT `attr foo`, bar FROM DOMAIN WHERE bar == "buz" AND `attr foo` between "4" and "1""2"}
      end

    end                         # "

    context "with a limit" do
      before :each do
        @it = Selection.new(@sdb, "DOMAIN",
          :attributes => [:foo, :bar], 
          :conditions => ["foo == 'bar'"],
          :limit      => 10)
      end

      it "should append a limit clause" do
        @it.to_s.should be == "SELECT foo, bar FROM DOMAIN WHERE foo == 'bar' LIMIT 10"
      end

      it "should be able to generate a count expression" do
        @it.count_expression.should be == "SELECT count(*) FROM DOMAIN WHERE foo == 'bar'"
      end
    end

    context "with an offset" do
      before :each do
        @it = Selection.new(@sdb, "DOMAIN",
          :attributes => [:foo, :bar], 
          :conditions => ["foo == 'bar'"],
          :limit      => 10,
          :offset     => 200)

        # Note: counts are relative to the last count, not absolute
        @first_results = count_results(100, "TOKEN1")
        @second_results = count_results(100, "TOKEN2")
        @third_results = count_results(100, nil)
        @sdb.stub!(:select).
          and_return(@first_results, @second_results, @third_results)
      end

      it "should be able to generate an offset count expression" do
        @it.offset_count_expression.should be == 
          "SELECT count(*) FROM DOMAIN WHERE foo == 'bar' LIMIT 200"
      end

      it "should be able to generate an offset count operation" do
        op = @it.offset_count_operation
        op.method.should be == :select
        op.args.should be == [
          "SELECT count(*) FROM DOMAIN WHERE foo == 'bar' LIMIT 200"
        ]
        op.starting_token.should be_nil
      end

      it "should use an SDB count to determine starting token" do
        @sdb = stub("SDB")
        @sdb.should_receive(:select).
          with(@it.offset_count_expression, nil).
          ordered.
          and_return(@first_results)
        @sdb.should_receive(:select).
          with(@it.offset_count_expression, "TOKEN1").
          ordered.
          and_return(@second_results)
        @sdb.should_not_receive(:select).
          with(@it.offset_count_expression, "TOKEN2")
        @it.sdb = @sdb
        @it.starting_token.should be == "TOKEN2"
      end

      it "should be able to generate a select operation" do
        op = @it.select_operation
        op.args.should be == [
          "SELECT foo, bar FROM DOMAIN WHERE foo == 'bar' LIMIT 10"
        ]
        op.starting_token.should be == "TOKEN2"
      end

      it "should be able to count items in the selection" do
        @sdb = stub("SDB")
        @sdb.should_receive(:select).
          with(@it.count_expression, "TOKEN2").
          and_return(count_results(4, "TOKEN5"))
        @sdb.should_receive(:select).
          with(@it.count_expression, "TOKEN5").
          and_return(count_results(5, nil))
        @it.sdb = @sdb
        @it.starting_token = "TOKEN2"
        @it.size.should be == 9
        @it.length.should be == 9
        @it.count.should be == 9
      end

      it "should be able to get selection results" do
        @sdb = stub("SDB")
        @sdb.should_receive(:select).
          with(@it.to_s, "TOKEN2").
          and_return(select_results(["foo", "bar"], "TOKEN5"))
        @sdb.should_receive(:select).
          with(@it.to_s, "TOKEN5").
          and_return(select_results(["baz", "buz"], nil))
        @it.sdb = @sdb
        @it.starting_token = "TOKEN2"
        @it.results.values.should be == [
          { "foo_attr" => ["foo_val"]},
          { "bar_attr" => ["bar_val"]},
          { "baz_attr" => ["baz_val"]},
          { "buz_attr" => ["buz_val"]}
        ]
        @it.results.keys.should be == [
          "foo", "bar", "baz", "buz"
        ]
      end
    end
  end
end
