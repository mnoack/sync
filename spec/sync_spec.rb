require 'merger'

describe Merger do
  before do
    # fill database
    create_tables
    db(:kis, "INSERT INTO resources (id, name, product_type_id) VALUES
    (1, 'KI Ferry', 1), (2, 'KI Boat', 1), (3, 'KI Insurance', 2)")
    db(:kis, "INSERT INTO product_types (id, name) VALUES
    (1, 'KI Transport'), (2, 'KI Other')")

    db(:qld, "INSERT INTO resources (id, name, product_type_id) VALUES
    (1, 'Q Ferry', 1), (2, 'Q Insurance', 2), (3, 'Q Boat', 1)")
    db(:qld, "INSERT INTO product_types (id, name) VALUES
    (1, 'Q Transport'), (2, 'Q Other')")
  end

  def create_tables
    master_db("DROP TABLE resources")
    db(:kis, "DROP TABLE resources")
    db(:qld, "DROP TABLE resources")
    master_db("DROP TABLE product_types")
    db(:kis, "DROP TABLE product_types")
    db(:qld, "DROP TABLE product_types")
    create_resources_sql = <<-SQL
      CREATE TABLE `resources` (
      id int(11) AUTO_INCREMENT,
      name varchar(20),
      product_type_id int(11),
      PRIMARY KEY(id)
    ) ENGINE=InnoDB
    SQL
    create_product_types_sql = <<-SQL
      CREATE TABLE `product_types` (
      id int(11) AUTO_INCREMENT,
      name varchar(20),
      PRIMARY KEY(id)
    ) ENGINE=InnoDB
    SQL
    db(:kis, create_resources_sql)
    db(:qld, create_resources_sql)
    db(:kis, create_product_types_sql)
    db(:qld, create_product_types_sql)
    create_resources_sql = <<-SQL
      CREATE TABLE `resources` (
      id int(11) AUTO_INCREMENT,
      client_id tinyint,
      name varchar(20),
      product_type_id int(11),
      PRIMARY KEY(id)
    ) ENGINE=InnoDB
    SQL
    create_product_types_sql = <<-SQL
      CREATE TABLE `product_types` (
      id int(11) AUTO_INCREMENT,
      client_id tinyint,
      name varchar(20),
      PRIMARY KEY(id)
    ) ENGINE=InnoDB
    SQL
    master_db(create_resources_sql)
    master_db(create_product_types_sql)
  end

  def db_config
    @db_config ||= YAML::load(File.read('database.test.yml'))
  end

  def db(tenant, command)
    ActiveRecord::Base.establish_connection(db_config['tenants'][tenant.to_s])
    execute command
  end

  def master_db(command)
    ActiveRecord::Base.establish_connection(db_config['master'])
    execute command
  end

  def execute(command)
    ActiveRecord::Base.connection.execute command
  end

  it 'should work' do
    tables = {
      resources: %w(id name product_type_id),
      product_types: %w(id name)
    }
    tenants = {kis: 1, ccc: 2, qld: 2}
    Merger.new(tables, tenants).run(:resources, :product_types)
    product_types = master_db("SELECT * FROM product_types").to_a
    expect(product_types).to eq ([
      [1, 1, 'KI Transport'],
      [2, 1, 'KI Other'],
      [3, 2, 'Q Transport'],
      [4, 2, 'Q Other'],
    ])
    resources = master_db("SELECT * FROM resources").to_a
    expect(resources).to eq ([
      #id client name    product_type
      [1, 1, 'KI Ferry',     1],
      [2, 1, 'KI Boat',      1],
      [3, 1, 'KI Insurance', 2],
      [4, 2, 'Q Ferry',      3],
      [5, 2, 'Q Insurance',  4],
      [6, 2, 'Q Boat',       3]
    ])
  end
end
