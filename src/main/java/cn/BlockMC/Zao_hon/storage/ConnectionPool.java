package cn.BlockMC.Zao_hon.storage;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Vector;

import org.bukkit.Bukkit;

// 看不透，为什么要自己写连接池呢，写出来肯定都有问题，shade 插件都有了，整个 HikariCP 之类只有100kb 的连接池不好吗 —— 754503921
public class ConnectionPool {
	private PoolConfig config;
	private Vector<Connection> pool;
	private Thread thread;
	private PoolRunnable runnable;

	public ConnectionPool(PoolConfig config) {
		this.config = config;
		// 可是这个构造方法传进去的 pool 肯定是 null 啊 —— 754503921
		runnable = new PoolRunnable(pool);
		thread = new Thread(runnable);
		thread.start();
	}

	public Connection getConnection() {
		if (pool == null) {
			pool = new Vector<Connection>();
		}
		Connection conn;
		if (pool.isEmpty()) {
			conn = createConnection();
		} else {
			int last = pool.size() - 1;
			conn = (Connection) pool.get(last);
			pool.remove(conn);
		}
		return conn;
	}

	public synchronized void releaseConnection(Connection conn) {

		if (pool.size() > config.getMaxConnection()) {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		} else {
			pool.add(conn);
		}
	}

	public Connection createConnection() {
		Connection conn = null;
		try {
			Class.forName(config.getDriverClassName());
			conn = DriverManager.getConnection(config.getJDBCUrl(), config.getUserName(), config.getPassworld());
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return conn;

	}

	public void close() {
		// runnable.cancel();
		runnable.stop();
		pool.forEach(conn -> {
			try {
				if (!conn.isClosed()) {
					conn.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
	}

	public Vector<Connection> getConnections() {
		return pool;
	}

	public PoolConfig getConfig() {
		return config;
	}

	class PoolRunnable implements Runnable {
		private boolean stop = false;
		private Vector<Connection> pool;

		public PoolRunnable(Vector<Connection> pool) {
			this.pool = pool;
		}

		@Override
		public void run() {
			this.poolRefresh();
		}

		public void poolRefresh() {
			while (!stop) {
				try {
					Thread.sleep(1000*60*60*4l);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				if (pool != null && !pool.isEmpty()) {
					int size = pool.size();
					Vector<Connection> v = new Vector<Connection>(pool.size());
					for (int i = 0; i < size; i++) {
						v.add(getConnection());
					}
					for (Connection conn : v) {
						PreparedStatement s;
						try {
							s = conn.prepareStatement(config.getTestQuery());
							s.execute();

						} catch (SQLException ignore) {
						} finally {
							releaseConnection(conn);
						}
					}
					Bukkit.getPluginManager().getPlugin("AnvilLogin").getLogger().info("conn test");
				}
			}
		}

		public void stop() {
			stop = true;
		}
	}

}
