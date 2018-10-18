package jobFiveMinute.subJob.AbnormalPorts.AbnormalPortsClass;

import java.util.Date;

public class ROWUtils {

	public static String genaralROW(){
		String row = (int)((Math.random()*9+1)*100000) + "";	//六位随机数
		row += new Date().getTime()/1000;
		return row;
	}
}
