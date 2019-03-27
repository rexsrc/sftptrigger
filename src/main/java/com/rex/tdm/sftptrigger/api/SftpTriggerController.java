package com.rex.tdm.sftptrigger.api;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/sftp-trigger")
public class SftpTriggerController implements ApplicationContextAware{
	
	private ConfigurableApplicationContext context;
	
	@GetMapping("/ping")
	public String ping() {
		return "OK";
	}
	

	@GetMapping("/sftp1/{action}")
	public Map<String,Object> setActionSftp1(
			HttpServletRequest request,
			HttpServletResponse response, 
			@PathVariable("action") String action){
		
		Map<String,Object> resp = new HashMap<String,Object>();
		
		MessageChannel controlChannel = this.context.getBean("controlBusChannel", MessageChannel.class);
		
		if (action!=null && action.equalsIgnoreCase("enable")) {//Enable
			controlChannel.send(new GenericMessage<String>("@'SFTPConfig.sftpMessageSource.inboundChannelAdapter'.start()"));
			System.err.println(System.currentTimeMillis() +"--sftpMessageSource enabled--");
			
		}else if (action!=null && action.equalsIgnoreCase("disable")) {//Disable
			controlChannel.send(new GenericMessage<String>("@'SFTPConfig.sftpMessageSource.inboundChannelAdapter'.stop()"));
			System.err.println(System.currentTimeMillis() +"--sftpMessageSource disabled--");
		}
		
		return resp;
	}
	
	@GetMapping("/sftp2/{action}")
	public Map<String,Object> setActionSftp2(
			HttpServletRequest request,
			HttpServletResponse response, 
			@PathVariable("action") String action){
		
		Map<String,Object> resp = new HashMap<String,Object>();
		
		MessageChannel controlChannel = this.context.getBean("controlBusChannel", MessageChannel.class);
		
		if (action!=null && action.equalsIgnoreCase("enable")) {//Enable
			controlChannel.send(new GenericMessage<String>("@'SFTPConfig.sftpMessageSource2.inboundChannelAdapter'.start()"));
			System.err.println(System.currentTimeMillis() +"--sftpMessageSource enabled--");
			
		}else if (action!=null && action.equalsIgnoreCase("disable")) {//Disable
			controlChannel.send(new GenericMessage<String>("@'SFTPConfig.sftpMessageSource2.inboundChannelAdapter'.stop()"));
			System.err.println(System.currentTimeMillis() +"--sftpMessageSource disabled--");
		}
		
		return resp;
	}


	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.context = (ConfigurableApplicationContext)applicationContext;
	}
	
}
