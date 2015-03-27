require 'yaml'
require 'active_record'
require 'mysql2'
require 'pry'

class Merger
  def initialize(tables, tenants)
    @tables = tables
    @tenants = tenants
  end

  def run(*tables)
    #create_lookup_table
    tables.each do |table|
      db_config['tenants'].keys.each do |tenant|
        rows = fetch_chunk(tenant.to_sym, table, 1, 10)
        puts rows.pretty_inspect
        insert_rows(tenant, table, rows)
      end
    end
    #insert_rows(rows)
  end

  def fetch_chunk(tenant, table, from_old_id, to_old_id)
    tenant_db(tenant)
    tenant_id = @tenants[tenant]
    q = "SELECT *, #{tenant_id} from #{table} WHERE id BETWEEN #{from_old_id} and #{to_old_id}"
    puts q
    res = ActiveRecord::Base.connection.execute q
    res.to_a
  end

  def insert_rows(tenant, table, rows)
    tenant_id = @tenants[tenant]
    table_id = @tables.keys.index(table)
  puts "ROWS**********"
  puts rows.inspect
  puts "ROWS END"
    @table = table
    master_db
    @last_id ||= {}
    @last_id[@table] ||= 0 # TODO: calculate property (e.g. max() in SQL)
    @translations = []
    sql  = <<-SQL
      INSERT INTO #{table} (#{@tables[table].join(', ')}, client_id)
      VALUES #{rows2values(rows, tenant_id, table_id)}
    SQL
    execute_sql(sql)
    execute_sql <<-SQL
      INSERT INTO lookup (client_id, old_id, new_id, table_id) VALUES #{raw_rows(@translations)}
    SQL
  end

  def next_id
    @last_id[@table] += 1
  end

  def raw_rows(rows)
    "(#{rows.map{|r| 
      r.map{|e| "'#{e}'"}.join(', ')
    }.join('), (')})"
  end

  def rows2values(rows, tenant_id, table_id)
    new_id = next_id
    "(#{rows.map{|r| 
      old_id = r.shift
      @translations << [old_id, new_id, tenant_id, table_id]
      next_id + ', ' + r.map{|e| "'#{e}'"}.join(', ')
    }.join('), (')})"
  end

  def create_lookup_table
    master_db
    puts "creating lookup tables"
    execute_sql <<-SQL
  CREATE TABLE IF NOT EXISTS `lookup` (
    `client_id` tinyint UNSIGNED NOT NULL,
    `old_id` int(11) UNSIGNED NOT NULL,
    `new_id` int(11) UNSIGNED NOT NULL,
    `table_id` tinyint UNSIGNED NOT NULL,
    PRIMARY KEY (`new_id`),
    KEY `lookup_client_id_table_id_old_id_index` (`client_id`,`table_id`,`old_id`),
    KEY `lookup_table_id_new_id_index` (`table_id`,`new_id`)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
    SQL
  end

  def db_config
    @db_config ||= YAML::load(File.read('database.test.yml'))
  end

  def execute_sql(sql)
    puts "executing #{sql}"
    ActiveRecord::Base.connection.execute sql
  end

  def master_db
    ActiveRecord::Base.establish_connection(db_config['master'])
  end

  def tenant_db(tenant)
    ActiveRecord::Base.establish_connection(db_config['tenants'][tenant.to_s])
  end
end

#loop do
#  max_audits = execute_sql("select IFNULL(max(id), 0) from audits").fetch_row.first.to_i
#  max_new_audits = execute_sql("select IFNULL(max(id), 0) from new_audits").fetch_row.first.to_i
#  puts "Loading from #{max_new_audits} up to #{max_audits}..."
#
#  first_id = max_new_audits + 1
#  last_id  = max_audits
#  (first_id..last_id).step(rows_to_chunk) do |from_id|
#    to_id = [from_id + rows_to_chunk - 1, max_audits].min
#
#    puts " => Chunk #{from_id} to #{to_id}"
#    load_chunk(from_id, to_id)
#  end
#end
