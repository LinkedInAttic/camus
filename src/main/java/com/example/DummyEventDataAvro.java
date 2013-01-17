package com.example;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

import com.edate.data.events.EventData.EventType;

public class DummyEventDataAvro
{
	private SpecificRecord m_data;
	
	private EventTypeAvro m_type;
	
	/**
	 * Holds Avro's event types and their schemas so that consumers 
	 * do not need to explicitly load it.
	 */
	private static Map<EventTypeAvro, Schema> m_schemas = new HashMap<EventTypeAvro, Schema>();
	
	static 
	{
		m_schemas.put(EventTypeAvro.DUMMY_LOG, DummyLog.SCHEMA$);
		m_schemas.put(EventTypeAvro.DUMMY_LOG_2, DummyLog2.SCHEMA$);
	}
	
	/**
	 * EventTypeAvro are event types in the pub/sub system
	 * where the published message is an avro type object serialized into a byte array.
	 * These EventTypes are meant to be used as validation for the types/format
	 * of messages we publish 
	 */
	public enum EventTypeAvro implements EventType
	{		 
		DUMMY_LOG,
		DUMMY_LOG_2;
		
		
		public Schema getSchema()
		{
			return m_schemas.get(this);
		}
	}
	
	public DummyEventDataAvro() 
	{}
	
	public DummyEventDataAvro(SpecificRecord data, EventTypeAvro type)
	{
		m_data = data;
		m_type = type;
	}
	
	public SpecificRecord getData()
	{
		return m_data;
	}
	
	public void setData(SpecificRecord data)
	{
		m_data = data;
	}
	
	public EventTypeAvro getType()
	{
		return m_type;
	}
	
	public void setType(EventTypeAvro type)
	{
		m_type = type;
	}
}
