package cn.tedu.storm.flux.dao;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MySqlDao {
	private static MySqlDao mySqlDao = new MySqlDao();
	
	private MySqlDao() {
	}
	
	public static MySqlDao getMySqlDao(){
		return mySqlDao;
	}
	
	public void  add3(TongJi3Info t3info){
		Connection conn =  null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try{
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://Park01:3306/fluxdb","root","root");
			conn.setAutoCommit(false);
			
			ps = conn.prepareStatement("insert into tongji3 values(?,?,?,?)");
			ps.setTimestamp(1, t3info.getTime());
			ps.setDouble(2, t3info.getBr());
			ps.setDouble(3, t3info.getAvgtime());
			ps.setDouble(4, t3info.getAvgdeep());
			
			ps.executeUpdate();
			conn.commit();
		}catch(Exception e){
			try {
				conn.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			} 
			e.printStackTrace();
		}finally {
			if(rs != null){
				try {
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}finally {
					rs = null;
				}
			}
			if(ps != null){
				try {
					ps.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}finally {
					ps = null;
				}
			}
			if(conn != null){
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}finally {
					conn = null;
				}
			}
		}
		
	}
	
	public void add2(TongJi2Info t2info){
		Connection conn =  null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try{
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://Park01:3306/fluxdb","root","root");
			conn.setAutoCommit(false);
			
			ps =conn.prepareStatement("select * from tongji2 where time  = ?");
			ps.setDate(1, new Date(t2info.getTime().getTime()));
			rs = ps.executeQuery();
			
			if(rs.next()){
				//有数据  - 更新
				ps = conn.prepareStatement("update tongji2 set pv= pv+?,uv=uv+?,vv=vv+?,newip=newip+?,newcust=newcust+? where time= ?");
				ps.setInt(1, t2info.getPv());
				ps.setInt(2, t2info.getUv());
				ps.setInt(3, t2info.getVv());
				ps.setInt(4, t2info.getNewip());
				ps.setInt(5, t2info.getNewcust());
				ps.setDate(6, new Date(t2info.getTime().getTime()));
				ps.executeUpdate();
			}else{
				//没数据 - 插入
				ps = conn.prepareStatement("insert into tongji2 values(?,?,?,?,?,?)");
				ps.setDate(1, new Date(t2info.getTime().getTime()));
				ps.setInt(2, t2info.getPv());
				ps.setInt(3, t2info.getUv());
				ps.setInt(4, t2info.getVv());
				ps.setInt(5, t2info.getNewip());
				ps.setInt(6, t2info.getNewcust());
				ps.executeUpdate();
			}
			
			conn.commit();
		}catch(Exception e){
			try {
				conn.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			} 
			e.printStackTrace();
		}finally {
			if(rs != null){
				try {
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}finally {
					rs = null;
				}
			}
			if(ps != null){
				try {
					ps.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}finally {
					ps = null;
				}
			}
			if(conn != null){
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}finally {
					conn = null;
				}
			}
		}
		
	}
	
}
