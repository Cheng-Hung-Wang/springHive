package my.springHive.controller;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.codehaus.jettison.json.JSONObject;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.gson.Gson;



@Controller
public class hiveOperation {
	
	@Inject
	private JdbcTemplate jdbcTemplate;
			
	//for web url
	@RequestMapping(value = "/select",
			        produces = "application/json;charset=UTF-8",  
			        method = RequestMethod.GET)
	@ResponseBody
	public String getwebdata() throws Exception{
		
		String tableName = "sample_07";
		String sql = "select * from " + tableName;
		List<Map<String, Object>> result = jdbcTemplate.queryForList(sql);
	    String resString = new String();
		JSONObject json = new JSONObject();
	    
		for (Map res:result){
			json.put(res.get(tableName+".code").toString(), res.get(tableName+".description"));
		}		
		return json.toString();
	}
	
	//for web url
	@RequestMapping(value = "/create/{data}",
					produces = "application/json;charset=UTF-8",  
			        method = RequestMethod.GET)
	@ResponseBody
	public String createTable(@PathVariable String data) throws Exception{
			
		String tableName = "hbase_" + data;
		
		//if(StringUtils.isEmpty(tableName))
		//	tableName = "demo";
		
		String sql = "drop table if exists hbase_" + tableName;
		
		jdbcTemplate.execute(sql);		
		
		sql = "create table " + tableName + "(key string, value string) " +
			  "stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' " +
			  "with serdeproperties ('hbase.columns.mapping' = ':key,description:itme') " +
			  "tblproperties('hbase.table.name' = '" +  tableName + "')";		
		jdbcTemplate.execute(sql);
		
		sql = "show tables";
		List<Map<String, Object>> result = jdbcTemplate.queryForList(sql);
		
		return  new Gson().toJson(result);
	}
	
	//for web url
	@RequestMapping(value = "/insert/{table}/{key}/{value}", 
					produces = "application/json;charset=UTF-8",
					method = RequestMethod.GET)
	@ResponseBody
	public String insertData(@PathVariable String table, @PathVariable String key, @PathVariable String value) throws Exception{
				
		table = "hbase_" + table;
		String sql = "insert into table " + table +  " values ('" + key + "','" + value + "')";	
		
		jdbcTemplate.execute(sql);		
		sql = "select * from " + table;	
		List<Map<String, Object>> result = jdbcTemplate.queryForList(sql);
	    String resString = new String();
		JSONObject json = new JSONObject();
	    
		for (Map res:result){
			json.put(res.get(table + ".key").toString(), res.get(table + ".value"));
		}
		
		return json.toString();
	}
	
	//for web url
	@RequestMapping(value = "/delete/{data}", method = RequestMethod.GET)
	@ResponseBody
	public String deleteTable(@PathVariable String data) throws Exception{
				
		data = "hbase_" + data;	
		String sql = "drop table if exists " + data;
					
		jdbcTemplate.execute(sql);
		sql = "show tables";
		List<Map<String, Object>> result = jdbcTemplate.queryForList(sql);
		
		return  new Gson().toJson(result);
	}

}
