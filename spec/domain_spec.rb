require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

module SDBTools
  describe Domain do
    before :each do
      @sdb = stub("SDB")
    end

    context "given a name and an SDB interface" do
      before :each do
        @it = Domain.new(@sdb, "foo")
      end

      it "should be able to get selected item attributes" do
        @sdb.should_receive(:get_attributes).
          with("foo", "bar", "baz")
          @it.get("bar", "baz")
      end

      it "should be able to delete selected item attributes" do
        @sdb.should_receive(:delete_attributes).
          with("foo", "bar", {"baz" => ["buz"]})
          @it.delete("bar", {"baz" => ["buz"]})
      end

      it "should be able to write selected item attributes" do
        @sdb.should_receive(:put_attributes).
          with("foo", "bar", {"baz" => ["buz"]}, false)
          @it.put("bar", {"baz" => ["buz"]})
      end

      it "should be able to overwrite selected item attributes" do
        @sdb.should_receive(:put_attributes).
          with("foo", "bar", {"baz" => ["buz"]}, :replace)
          @it.put("bar", {"baz" => ["buz"]}, :replace => true)
      end
    end
  end
end
