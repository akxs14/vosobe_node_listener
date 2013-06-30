#require the following gems:
#   msgpack
#   msgpack-rpc
#   redis
#   pg
require 'msgpack'
require 'msgpack/rpc'
require 'redis'
require 'pg'
require 'securerandom'
require 'rbconfig'

class NodeMonitorHandler

  #
  # => API Calls
  #
  def initialize
    @redis_srv = connect_redis
    @postgres_srv = connect_postgres
  end

  def receive_stats id, ip_addr, cpu_load, free_ram
    puts "Node id: #{id}"
    puts "IP Address: #{ip_addr}"
    puts "CPU Load: #{cpu_load}"
    puts "Free RAM: #{free_ram}"
    puts "\n"
    save_pm_metrics id, ip_addr, cpu_load, free_ram
    add_ram_score id, free_ram
    add_cpu_score id, cpu_load
    return 0
  end

  def get_new_id ip_addr
    @redis_srv.incr("pms:id")
    new_id = @redis_srv.get("pms:id")   
    create_postgres_entry new_id.to_i, ip_addr
    new_id.to_i
  end

  def is_hypervisor_registered pm_id
    find_hypervisor_entry(pm_id)
  end

  def register_hypervisor(model, memory, cpus, mhz, numa_nodes, sockets, cores,
    threads, pm_id)
    puts "inside register_hypervisor" 
    insert_hypervisor_postgres(@postgres_srv, model, memory, cpus, mhz, numa_nodes, sockets, 
      cores, threads, pm_id)
    return 0
  end
  
  #
  # => Internal methods
  #
  private

    def connect_postgres(host = "127.0.0.1", port = 5432, db_name = "vosobe_development",
      user = 'makis', pass = 'akxs14' )
      conn = PG::Connection.connect(host, port, nil, nil, db_name,user, pass)
      conn
    end

    def create_postgres_entry id, ip_addr
      insert_pm_postgres @postgres_srv, id, ip_addr
    end

    def find_hypervisor_entry pm_id
      result = @postgres_srv.exec('
        SELECT id 
        FROM hypervisors 
        WHERE physical_machine_id = $1',[pm_id])

      puts "find_hypervisor_entry, #{result}, #{result.inspect}"

      result.each do |row|
        result.clear
        return 1
      end

      result.clear
      return 0
    end

    def insert_pm_postgres connection, id, ip_addr
      connection.prepare("statement_insert_pm", 
        "insert into physical_machines (id, uuid, os, 
          name, ip_address, created_at, updated_at) 
        values($1, $2, $3, $4, $5, $6, $7)")
      connection.exec_prepared("statement_insert_pm",
        [id,
         SecureRandom.uuid,
         RbConfig::CONFIG['host_os'],
         `hostname`,
         ip_addr,
         Time.now.getutc,
         Time.now.getutc
        ])
    end

    def insert_hypervisor_postgres(connection, model, memory, cpus, mhz, numa_nodes, sockets, 
      cores, threads, pm_id)
      puts "prepare statement"
      connection.prepare("statement_insert_hyperv", 
        "insert into hypervisors (model, memory, cpus, mhz, numa_nodes, sockets,
          cores, threads, physical_machine_id, created_at, updated_at) 
        values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)")
      puts "execute statement"
      connection.exec_prepared("statement_insert_hyperv",
        [model,
         memory,
         cpus,
         mhz,
         numa_nodes,
         sockets,
         cores,
         threads,
         pm_id,
         Time.now.getutc,
         Time.now.getutc
        ])
    end

    def connect_redis host = "127.0.0.1", port = 6379
      redis = Redis.new(:host => host, :port => port)
      redis
    end

    def save_pm_metrics id, ip_addr, cpu_load, free_ram
      @redis_srv.hmset("pms:#{id}", "id", id, "ip_addr", ip_addr, "cpu_load", cpu_load, "free_ram", free_ram)
    end

    def add_ram_score id, free_ram
      @redis_srv.zadd("pms:score:free_ram", free_ram, id)
    end

    def add_cpu_score id, cpu_load
      @redis_srv.zadd("pms:score:cpu_load", cpu_load, id)
    end
end
