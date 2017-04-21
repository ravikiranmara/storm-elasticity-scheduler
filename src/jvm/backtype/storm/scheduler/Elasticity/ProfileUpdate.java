package backtype.storm.scheduler.Elasticity;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;

import com.sun.management.OperatingSystemMXBean;


public class ProfileUpdate implements Runnable {

	@Override
	public void run() {
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
		OperatingSystemMXBean bean=(OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
		Runtime rt=Runtime.getRuntime();
		while(true){
			//update cpu usage
			Slave.prf.setCpu_usage(bean.getSystemCpuLoad());
			//Slave.prf.setBandwidth_in(0);
			//Slave.prf.setBandwidth_out(0);
			
			
			try {
				//oct in
				String[] cmd = {"/bin/sh","-c","netstat -s | grep -i InOctets | cut -f2 -d':'"};
				Process p = rt.exec(cmd);
				InputStreamReader isr = new InputStreamReader(p.getInputStream());
				BufferedReader br = new BufferedReader(isr);  
				String s = null;  
				while ((s = br.readLine()) != null) {  
				    /* do someting with 's' */ 
					s=s.replaceAll("\\s", "");
					int i=Integer.parseInt(s);
					double d=(double)i;
					double old=Slave.prf.getCurrent_inoctets();
					Slave.prf.setBandwidth_in((d-old)/(1000*1024*1024)); 
					Slave.prf.setCurrent_inoctets(d);
					
				}  
				p.waitFor();
				//oct out
				String[] cmd_out = {"/bin/sh","-c","netstat -s | grep -i OutOctets | cut -f2 -d':'"};
				p = rt.exec(cmd_out);
				isr = new InputStreamReader(p.getInputStream());
				br = new BufferedReader(isr);  
				s = null;  
				while ((s = br.readLine()) != null) {  
				    /* do someting with 's' */ 
					s=s.replaceAll("\\s", "");
					int i=Integer.parseInt(s);
					double d=(double)i;
					double old=Slave.prf.getCurrent_outoctets();
					Slave.prf.setBandwidth_out((d-old)/(1000*1024*1024)); 
					Slave.prf.setCurrent_outoctets(d);
				}  
				p.waitFor();
				//wait for 1s
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
