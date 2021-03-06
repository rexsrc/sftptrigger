package com.rex.tdm.sftptrigger.config;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.annotation.Transformer;
import org.springframework.integration.config.ExpressionControlBusFactoryBean;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.file.FileHeaders;
import org.springframework.integration.file.filters.CompositeFileListFilter;
import org.springframework.integration.file.filters.FileListFilter;
import org.springframework.integration.file.filters.FileSystemPersistentAcceptOnceFileListFilter;
import org.springframework.integration.file.filters.SimplePatternFileListFilter;
import org.springframework.integration.file.remote.session.CachingSessionFactory;
import org.springframework.integration.file.remote.session.SessionFactory;
import org.springframework.integration.jdbc.metadata.JdbcMetadataStore;
import org.springframework.integration.metadata.ConcurrentMetadataStore;
import org.springframework.integration.sftp.filters.SftpPersistentAcceptOnceFileListFilter;
import org.springframework.integration.sftp.filters.SftpSimplePatternFileListFilter;
import org.springframework.integration.sftp.inbound.SftpInboundFileSynchronizer;
import org.springframework.integration.sftp.inbound.SftpInboundFileSynchronizingMessageSource;
import org.springframework.integration.sftp.inbound.SftpStreamingMessageSource;
import org.springframework.integration.sftp.session.DefaultSftpSessionFactory;
import org.springframework.integration.sftp.session.SftpRemoteFileTemplate;
import org.springframework.integration.transformer.StreamTransformer;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHandlingException;
import org.springframework.messaging.MessagingException;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;

@Configuration
public class SFTPConfig {

    @Value("${sftp.host}")
    private String sftpHost;

    @Value("${sftp.port:22}")
    private int sftpPort;

    @Value("${sftp.user}")
    private String sftpUser;

    @Value("${sftp.password:#{null}}")
    private String sftpPasword;
 
    @Value("${sftp.remote.directory.download:/}")
    private String sftpRemoteDirectoryDownload;
 
    @Value("${sftp.local.directory.download:${java.io.tmpdir}/localDownload}")
    private String sftpLocalDirectoryDownload;
 
    @Value("${sftp.remote.directory.download.filter:*.*}")
    private String sftpRemoteDirectoryDownloadFilter;
    
    @Autowired
    private DataSource dataSource;
    
    @Bean
    @ServiceActivator(inputChannel = "controlBusChannel")
    public ExpressionControlBusFactoryBean controlBus() throws Exception {
        ExpressionControlBusFactoryBean controlBus = new ExpressionControlBusFactoryBean();
        return controlBus;
    }
    
    @Bean
    public SessionFactory<LsEntry> sftpSessionFactory() {
        DefaultSftpSessionFactory factory = new DefaultSftpSessionFactory(true);
        factory.setHost(sftpHost);
        factory.setPort(sftpPort);
        factory.setUser(sftpUser);

        factory.setPassword(sftpPasword);
        factory.setAllowUnknownKeys(true);
        return new CachingSessionFactory<LsEntry>(factory);
    }
    
    @Bean
    public ConcurrentMetadataStore metadataStore() {
    	JdbcMetadataStore meta = new JdbcMetadataStore(dataSource);
    	return meta;
    }
    
    @Bean
    public SftpInboundFileSynchronizer sftpInboundFileSynchronizer() {
    	SftpInboundFileSynchronizer fileSynchronizer = new SftpInboundFileSynchronizer(sftpSessionFactory());
        fileSynchronizer.setDeleteRemoteFiles(false);
        fileSynchronizer.setRemoteDirectory(sftpRemoteDirectoryDownload);
        
        
        SftpPersistentAcceptOnceFileListFilter sftpPersistFilter = new SftpPersistentAcceptOnceFileListFilter(metadataStore(),"sftp:");
        
        SftpSimplePatternFileListFilter sftpSimpleFilter = new SftpSimplePatternFileListFilter("*.pem");
        
        List<FileListFilter<ChannelSftp.LsEntry>> filters = new ArrayList<FileListFilter<ChannelSftp.LsEntry>>();
        
        filters.add(sftpPersistFilter);
        filters.add(sftpSimpleFilter);
        
        CompositeFileListFilter<ChannelSftp.LsEntry> comp = new CompositeFileListFilter<ChannelSftp.LsEntry>(filters);
        fileSynchronizer
        		.setFilter(comp);
        return fileSynchronizer;
    }
    
    
    @Bean
    @InboundChannelAdapter(channel = "fromSftpChannel", poller = @Poller(fixedDelay = "${sftp.poller.fixed-delay-ms:5000}"), autoStartup="false")
    public MessageSource<File> sftpMessageSource() {
        SftpInboundFileSynchronizingMessageSource source = new SftpInboundFileSynchronizingMessageSource(
                sftpInboundFileSynchronizer());

        source.setLocalDirectory(new File(sftpLocalDirectoryDownload));
        source.setAutoCreateLocalDirectory(true);
        
        FileSystemPersistentAcceptOnceFileListFilter filePersistFilter = new FileSystemPersistentAcceptOnceFileListFilter(metadataStore(),"file:");
        
        SimplePatternFileListFilter simpleFilter = new SimplePatternFileListFilter("*.pem");
        
        List<FileListFilter<File>> filters = new ArrayList<FileListFilter<File>>();
        
        filters.add(simpleFilter);
        filters.add(filePersistFilter);
        
        CompositeFileListFilter<File> comp = new CompositeFileListFilter<File>(filters);
        
        
        source.setLocalFilter(comp);
        source.setMaxFetchSize(1);
        return source;
    }
    
    @Bean
    @ServiceActivator(inputChannel = "fromSftpChannel")
    public MessageHandler resultFileHandler() {
        
    	return new MessageHandler() {
            @Override
            public void handleMessage(Message<?> message) throws MessagingException {
                System.err.println(System.currentTimeMillis() +"--1--"+ message.getPayload());
            }
        };
    }
    
    //-------------------------------------------------------------------//
    
    
    @Bean
    public SftpInboundFileSynchronizer sftpInboundFileSynchronizer2() {
    	SftpInboundFileSynchronizer fileSynchronizer = new SftpInboundFileSynchronizer(sftpSessionFactory());
        fileSynchronizer.setDeleteRemoteFiles(false);
        fileSynchronizer.setRemoteDirectory("/SFTP_DATA/APP_IN/tdmdtt/dat/out/interface/spring2");
        
        
        
        SftpPersistentAcceptOnceFileListFilter sftpPersistFilter = new SftpPersistentAcceptOnceFileListFilter(metadataStore(),"sftp2:");
        
        SftpSimplePatternFileListFilter sftpSimpleFilter = new SftpSimplePatternFileListFilter("*.cer");
        
        List<FileListFilter<ChannelSftp.LsEntry>> filters = new ArrayList<FileListFilter<ChannelSftp.LsEntry>>();
        
        filters.add(sftpPersistFilter);
        filters.add(sftpSimpleFilter);
        
        CompositeFileListFilter<ChannelSftp.LsEntry> comp = new CompositeFileListFilter<ChannelSftp.LsEntry>(filters);
        fileSynchronizer
        		.setFilter(comp);
        
        return fileSynchronizer;
    }
    
    @Bean
    @InboundChannelAdapter(channel = "fromSftpChannel2", poller = @Poller(fixedDelay = "${sftp.poller.fixed-delay-ms:5000}"), autoStartup="false")
    public MessageSource<File> sftpMessageSource2() {
        SftpInboundFileSynchronizingMessageSource source = new SftpInboundFileSynchronizingMessageSource(
                sftpInboundFileSynchronizer2());

        source.setLocalDirectory(new File("D:\\Temp\\tdm2"));
        source.setAutoCreateLocalDirectory(true);
        
        FileSystemPersistentAcceptOnceFileListFilter filePersistFilter = new FileSystemPersistentAcceptOnceFileListFilter(metadataStore(),"file2:");
        
        SimplePatternFileListFilter simpleFilter = new SimplePatternFileListFilter("*.cer");
        
        List<FileListFilter<File>> filters = new ArrayList<FileListFilter<File>>();
        
        filters.add(simpleFilter);
        filters.add(filePersistFilter);
        
        CompositeFileListFilter<File> comp = new CompositeFileListFilter<File>(filters);
        
        
        source.setLocalFilter(comp);
        source.setMaxFetchSize(2);
        return source;
    }
    
    @Bean
    @ServiceActivator(inputChannel = "fromSftpChannel2")
    public MessageHandler resultFileHandler2() {
        
    	return new MessageHandler() {
            @Override
            public void handleMessage(Message<?> message) throws MessagingException {
                System.err.println(System.currentTimeMillis() +"--2--"+ message.getPayload());
            }
        };
    }
    
    
  //-------------------------------------------------------------------//
    
    
    @ServiceActivator(inputChannel= "errorChannel" )
    public void handleErrorMessage(Message<MessageHandlingException> message) {
    	System.err.println(System.currentTimeMillis() +"--ERR--"+ message.getPayload().getCause());
    }
    
    
  //-------------------------------------------------------------------//
    
    
    
    @Bean
    @InboundChannelAdapter(channel = "streamSftpChannel", poller = @Poller(fixedDelay = "3000"), autoStartup="true")
    public MessageSource<InputStream> sftpMessageStreamSource(){
    	SftpStreamingMessageSource messageSource = new SftpStreamingMessageSource(sftpRemoteFileTemplate());
        messageSource.setRemoteDirectory("/SFTP_DATA/APP_IN/tdmdtt/dat/out/interface/spring3");
        
        SftpPersistentAcceptOnceFileListFilter sftpPersistFilter = new SftpPersistentAcceptOnceFileListFilter(metadataStore(),"sftp3:");
        
        SftpSimplePatternFileListFilter sftpSimpleFilter = new SftpSimplePatternFileListFilter("*.log");
        
        List<FileListFilter<ChannelSftp.LsEntry>> filters = new ArrayList<FileListFilter<ChannelSftp.LsEntry>>();
        
        filters.add(sftpPersistFilter);
        filters.add(sftpSimpleFilter);
        
        CompositeFileListFilter<ChannelSftp.LsEntry> comp = new CompositeFileListFilter<ChannelSftp.LsEntry>(filters);
        
        messageSource.setFilter(comp);
        messageSource.setMaxFetchSize(1); /// do not change this value in cluster environment
        return messageSource;
    }
    
    @Bean
    public SftpRemoteFileTemplate sftpRemoteFileTemplate() {
        return new SftpRemoteFileTemplate(sftpSessionFactory());
    }
    
    /*@Bean  // for downloading the content
	@Transformer(inputChannel = "streamSftpChannel", outputChannel = "streamDataChannel")
	public org.springframework.integration.transformer.Transformer transformer() {
		return new StreamTransformer("UTF-8"); /// to make automatically close the session
	}*/
    
    @Bean
    @ServiceActivator(inputChannel = "streamSftpChannel")
    public MessageHandler resultStreamHandler3() {
        
    	return new MessageHandler() {
            @Override
            public void handleMessage(Message<?> message) throws MessagingException {
            	System.err.println(System.currentTimeMillis() +"--3--getHeaders.FileHeaders.REMOTE_DIRECTORY:"+ message.getHeaders().get(FileHeaders.REMOTE_DIRECTORY));
            	System.err.println(System.currentTimeMillis() +"--3--getHeaders.FileHeaders.REMOTE_FILE:"+ message.getHeaders().get(FileHeaders.REMOTE_FILE));
            	System.err.println(System.currentTimeMillis() +"--3--getHeaders.FileHeaders.REMOTE_FILE_INFO:"+ message.getHeaders().get(FileHeaders.REMOTE_FILE_INFO));
                
            	Object obj = (Object)message.getPayload();
            	
            	if (obj instanceof InputStream) {
            		InputStream is = (InputStream)obj;
            		try {
						is.close();
						System.err.println(System.currentTimeMillis() +"--3--getPayload.Closed");
					} catch (IOException e) {
						e.printStackTrace();
					}
            	}
            	
            }
        };
    }
	
}
