package com.pushtechnology;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pushtechnology.diffusion.api.APIException;
import com.pushtechnology.diffusion.api.data.TopicDataFactory;
import com.pushtechnology.diffusion.api.data.metadata.MDataType;
import com.pushtechnology.diffusion.api.data.metadata.MRecord;
import com.pushtechnology.diffusion.api.data.metadata.MetadataFactory;
import com.pushtechnology.diffusion.api.data.paging.PagedRecordTopicData;
import com.pushtechnology.diffusion.api.message.Record;
import com.pushtechnology.diffusion.api.message.TopicMessage;
import com.pushtechnology.diffusion.api.publisher.Client;
import com.pushtechnology.diffusion.api.publisher.Publisher;
import com.pushtechnology.diffusion.api.topic.Topic;

public class PagedRecordTopicDataExample extends Publisher {
    private static final Logger LOG = LoggerFactory.getLogger(PagedRecordTopicDataExample.class);

    /** Topics created for this publisher. */
    public static final String PUSH_TOPIC = "PushTechnology";
    public static final String FX_ADD_TOPIC = "FX-Add";
    public static final String FX_UPDATE_TOPIC = "FX-Update";
    
    private Topic pushTopic;
    private Topic fxAddTopic;
    private Topic fxUpdateTopic;
    private Record recordUSDEUR;
    private Record recordUSDGBP;
    private Record recordEURGBP;
    private Record recordUSDAUD;
    private MRecord recordMetadata;
    private PagedRecordTopicData prAddData;
    private PagedRecordTopicData prUpdateData;
    private boolean isAddFeedStarted;
    private boolean isUpdateFeedStarted;
    private SimpleDateFormat dateFormat;

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    @Override
    protected void initialLoad() throws APIException {
        dateFormat = new SimpleDateFormat("HH:mm:ss");
        recordMetadata = MetadataFactory.newRecordMetadata("RecordMetadata");
        recordMetadata.addField("Currency");
        recordMetadata.addField("Price",MDataType.DECIMAL_STRING);
        recordMetadata.addField("Date");
        
        /*
         * prAddData and prUpdateData represent the topic data I will be using
         * as an example of the different functions of PagedRecordTopicData.
         */
        prAddData = TopicDataFactory.newPagedRecordData(recordMetadata);
        prUpdateData = TopicDataFactory.newPagedRecordData(recordMetadata);
        
        pushTopic = addTopic(PUSH_TOPIC);
        fxAddTopic = pushTopic.addTopic(FX_ADD_TOPIC, prAddData);
        fxUpdateTopic = pushTopic.addTopic(FX_UPDATE_TOPIC, prUpdateData);
        isAddFeedStarted = false;
        isUpdateFeedStarted = false;
    }

    @Override
    protected boolean isStoppable() {
        return true;
    }

    @Override
    protected void subscription(Client client, Topic topicSubscribed, boolean loaded)
        throws APIException {

        if (!loaded) {
            if (topicSubscribed.hasData()) {
                client.send(topicSubscribed.getData().getLoadMessage(client));
            }
            else {
                if( LOG.isTraceEnabled() ) {
                    LOG.trace(
                        "No action in subscription method for topic '{}'.",
                        topicSubscribed);
                }
            }
        }

        final TopicMessage m = topicSubscribed.createLoadMessage();
        String currentDateTime = dateFormat.format(new Date());
        recordUSDEUR = new Record(recordMetadata, "USDEUR", "0.0", currentDateTime);
        recordUSDGBP = new Record(recordMetadata, "USDGBP", "0.0", currentDateTime);
        recordEURGBP = new Record(recordMetadata, "EURGBP", "0.0", currentDateTime);
        recordUSDAUD = new Record(recordMetadata, "USDAUD", "0.0", currentDateTime);

        if(topicSubscribed.getName().equals("PushTechnology/FX-Add")) {
            m.putRecords(recordUSDEUR, recordUSDGBP);
            if(!isAddFeedStarted) {
                generateAddFeed();
            }
        }
        if(topicSubscribed.getName().equals("PushTechnology/FX-Update")) {
            m.putRecords(recordEURGBP, recordUSDAUD);
            if(!isUpdateFeedStarted) {
                prUpdateData.add(recordUSDEUR);
                prUpdateData.add(recordUSDGBP);
                generateUpdateFeed();
            }
        }

        client.send(m);
    }

    /**
     * Generates data for records to be added to the page.
     */
    private void generateAddFeed() {
        executor.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                try {
                    recordUSDEUR.setField("Price", Double.toString(generateFXValue()));
                    recordUSDGBP.setField("Price", Double.toString(generateFXValue()));
                    recordUSDEUR.setField("Date", dateFormat.format(new Date()));
                    recordUSDGBP.setField("Date", dateFormat.format(new Date()));
                    prAddData.add(recordUSDEUR);
                    prAddData.add(recordUSDGBP);
                    final TopicMessage topicData = fxAddTopic.createDeltaMessage();
                    topicData.putRecords(prAddData.getLines());
                    fxAddTopic.publishMessage(topicData);

                    //When the topic has reached over a certain number of lines, we can reset 
                    //it 
                    if(prAddData.getNumberOfLines() >= 10 ) {
                        prAddData.remove(0, prAddData.getNumberOfLines());
                    }
                }
                catch (APIException e) {
                    LOG.debug("Exception:", e);
                }
            }
        }, 5, 5, TimeUnit.SECONDS);

        isAddFeedStarted = true;
    }
    
    /**
     * Generates data for records already existing in a page
     */
    private void generateUpdateFeed() {
        executor.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                try {
                    recordEURGBP.setField("Price", Double.toString(generateFXValue()));
                    recordUSDAUD.setField("Price", Double.toString(generateFXValue()));
                    recordEURGBP.setField("Date", dateFormat.format(new Date()));
                    recordUSDAUD.setField("Date", dateFormat.format(new Date()));
                    prUpdateData.update(0, recordEURGBP);
                    prUpdateData.update(1, recordUSDAUD);

                    final TopicMessage topicData = fxUpdateTopic.createDeltaMessage();
                    topicData.putRecords(prUpdateData.getLines());
                    fxUpdateTopic.publishMessage(topicData);
                }
                catch (APIException e) {
                    LOG.debug("Exception:", e);
                }
            }
        }, 5, 5, TimeUnit.SECONDS);

        isUpdateFeedStarted = true;
    }
    
    private double generateFXValue() {
        double lower = 2000;
        double upper = 1500;
        return (Math.random() * (upper - lower) + lower)/1000;
    }
}
