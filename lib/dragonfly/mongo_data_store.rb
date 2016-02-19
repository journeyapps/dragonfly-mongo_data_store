require 'mongo'
require 'dragonfly'

Dragonfly::App.register_datastore(:mongo){ Dragonfly::MongoDataStore }

module Dragonfly
  class MongoDataStore

    include Serializer

    def initialize(opts={})
      @hosts     = opts[:hosts]
      @options   = opts[:options]
      @database  = opts[:database]
      @client    = opts[:client]
    end

    attr_accessor :client
    attr_reader :hosts, :options, :database

    def write(content, opts={})
      content.file do |f|
        grid_file = Mongo::Grid::File.new(f.read, filename: content.name, metadata: content.meta.merge(content_type: content.mime_type))
        mongo_id = client.database.fs.insert_one(grid_file)
        mongo_id.to_s
      end
    end

    def read(uid)
      grid_io = client.database.fs.find_one(_id: bson_id(uid))
      unless grid_io.nil?
        meta = extract_meta(grid_io)
        [grid_io.data, meta]
      end
    rescue BSON::ObjectId::Invalid => e
      nil
    end

    def destroy(uid)
      file = client.database.fs.find_one(_id: bson_id(uid))
      client.database.fs.delete_one(file) unless file.nil?
    rescue BSON::ObjectId::Invalid => e
      Dragonfly.warn("#{self.class.name} destroy error: #{e}")
    end

    def client
      default_options = {
        database: 'dragonfly'
      }
      mongo_options = default_options.merge(@options || {})
      hosts = @hosts || ['localhost:27017']
      @client ||= Mongo::Client.new(hosts, mongo_options)
    end

    def gridfs
      client.database.fs
    end

    private

    def bson_id(uid)
      BSON::ObjectId.from_string(uid)
    end

    def extract_meta(grid_io)
      meta = grid_io.info.metadata
      meta = Utils.stringify_keys(marshal_b64_decode(meta)) if meta.is_a?(String) # Deprecated encoded meta
      meta.merge!('stored_at' => grid_io.upload_date)
      meta
    end

  end
end

