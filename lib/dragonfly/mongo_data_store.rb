require 'mongo'
require 'dragonfly'

Dragonfly::App.register_datastore(:mongo){ Dragonfly::MongoDataStore }

module Dragonfly
  class MongoDataStore

    include Serializer

    def initialize(opts={})
      @host            = opts[:host] || 'localhost'
      @port            = opts[:port] || 27017
      @hosts           = opts[:hosts]
      @connection_opts = opts[:connection_opts] || {}
      @database        = opts[:database] || 'dragonfly'
      @username        = opts[:username]
      @password        = opts[:password]
      @client          = opts[:client]
    end

    attr_accessor :host, :hosts, :connection_opts, :port, :database, :username, :password

    def write(content, opts={})
      content.file do |f|
        data = f.read
        grid_file = Mongo::Grid::File.new(data, filename: content.name, content_type: content.mime_type, metadata: content.meta)
        if data.length == 0
          # HACK for mongo 2.0 that can't handle empty files.
          # For this case, no chunks should be created - only the file metadata is saved.
          # Avoid this hack in Mongo 2.1+
          client.database.fs.files_collection.insert_one(grid_file.metadata)
          mongo_id = grid_file.id
        else
          mongo_id = client.database.fs.insert_one(grid_file)
        end
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
        database: @database
      }
      default_options[:user] = username if username
      default_options[:password] = password if password
      @hosts ||= ["#{host}:#{port}"]
      @client ||= Mongo::Client.new(hosts, default_options.merge(connection_opts))
    end

    def gridfs
      client.database.fs
    end

    private

    def bson_id(uid)
      BSON::ObjectId.from_string(uid)
    end

    def extract_meta(grid_io)
      meta = grid_io.metadata.metadata
      meta = Utils.stringify_keys(marshal_b64_decode(meta)) if meta.is_a?(String) # Deprecated encoded meta
      meta.merge!('stored_at' => grid_io.upload_date)
      meta
    end

  end
end
