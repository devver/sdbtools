require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

module SDBTools
  describe Operation do
    before :each do
      @sdb = stub("SDB")
    end

    context "given a starting next token" do
      before :each do
        @it = Operation.new(@sdb, :select, "ARG", :starting_token => "TOKEN")
      end

      it "should pass the token to the first call" do
        @sdb.should_receive(:select).with("ARG", "TOKEN")
        @it.each do break; end
      end
    end

    it "should pass itself into the given block" do
      op = :unset
      @sdb.stub!(:select).and_return({})
      @it = Operation.new(@sdb, :select, "ARG")
      op = @it.each do |results, operation|
        break operation
      end
      op.should equal(@it)
    end
  end
end
