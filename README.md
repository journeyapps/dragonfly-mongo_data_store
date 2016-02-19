# Dragonfly::MongoDataStore

Mongo data store for use with the [Dragonfly](http://github.com/markevans/dragonfly) gem.

## Gemfile

```ruby
gem 'dragonfly-mongo_data_store'
```

## Usage

Configuration, with default options (remember the require)

```ruby
require 'dragonfly/mongo_data_store'

Dragonfly.app.configure do
  # ...

  datastore :mongo

  # ...
end
```

Or with options:

```ruby
datastore :mongo, hosts: ['my.host:27017'], options: {database: 'my_database'}
```

### Available options

```ruby
:hosts      # A list of host+ports (eg. ['n1.mydb.net:27017', 'n2.mydb.net:27017']) or a mongo connection string (eg. 'mongodb://127.0.0.1:27017/mydb?replicaSet=myapp')
:options    # Mongo::Client options. see https://docs.mongodb.org/ecosystem/tutorial/ruby-driver-tutorial/#client-options
```

