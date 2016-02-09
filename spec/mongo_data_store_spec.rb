# encoding: utf-8
require 'spec_helper'
require 'dragonfly/mongo_data_store'
require 'dragonfly/spec/data_store_examples'
require 'mongo'

Mongo::Logger.logger.level = Logger::WARN

describe Dragonfly::MongoDataStore do

  let(:app) { Dragonfly.app }
  let(:content) { Dragonfly::Content.new(app, "Pernumbucano") }
  let(:new_content) { Dragonfly::Content.new(app) }

  before(:each) do
    @data_store = Dragonfly::MongoDataStore.new :database => 'dragonfly_test'
  end

  describe "configuring the app" do
    it "can be configured with a symbol" do
      app.configure do
        datastore :mongo
      end
      app.datastore.should be_a(Dragonfly::MongoDataStore)
    end
  end

  it_should_behave_like 'data_store'

  describe "connecting to a replica set" do
    it "should initiate a replica set connection if hosts is set" do
      @data_store.hosts = ['1.2.3.4:27017', '1.2.3.4:27017']
      @data_store.connection_opts = {:replica_set => 'testingset'}
      Mongo::Client.should_receive(:new).with(['1.2.3.4:27017', '1.2.3.4:27017'], database: 'dragonfly_test', replica_set: 'testingset')
      @data_store.client
    end
  end

  describe "sharing already configured stuff" do
    before(:each) do
      @client = Mongo::Client.new(['localhost:27017'], database: 'dragonfly_test_yo')
      @database = @client.database
    end

    it "should allow sharing the client" do
      data_store = Dragonfly::MongoDataStore.new :client => @client
      @client.should_receive(:database).and_call_original
      data_store.client.database.should == @database
    end
  end

  describe "content type" do
    it "should be available in the metadata (taken from ext)" do
      content.name = 'text.txt'
      uid = @data_store.write(content)
      data, meta = @data_store.read(BSON::ObjectId.from_string(uid));
      meta[:content_type].should == 'text/plain'
      data.should == content.data
    end
  end


  describe "write and reads" do
    it "works for content" do
      content = Dragonfly::Content.new(app, "gollum")
      uid = @data_store.write(content)
      stuff, meta = @data_store.read(uid)
      retrieved_content = Dragonfly::Content.new(app, stuff, meta)
      retrieved_content.data.should == "gollum"
    end

    it "works for empty file" do
      content = Dragonfly::Content.new(app, "")
      uid = @data_store.write(content)
      stuff, meta = @data_store.read(uid)
      retrieved_content = Dragonfly::Content.new(app, stuff, meta)
      retrieved_content.data.should == ""
    end
  end


  describe "already stored stuff" do
    it "still works" do
      grid_file = Mongo::Grid::File.new("DOOBS", filename: 'pre-existing', metadata: {'some' => 'meta'})
      uid = @data_store.gridfs.insert_one(grid_file).to_s
      new_content.update(*@data_store.read(uid))
      new_content.data.should == "DOOBS"
      new_content.meta['some'].should == 'meta'
    end

    it "still works when meta was stored as a marshal dumped hash (but stringifies keys)" do
      grid_file = Mongo::Grid::File.new("DOOBS", filename: 'pre-existing', metadata: Dragonfly::Serializer.marshal_b64_encode(:some => 'stuff'))
      uid = @data_store.gridfs.insert_one(grid_file).to_s
      c, meta = @data_store.read(uid)
      meta['some'].should == 'stuff'
    end
  end

end
