
//this method is used to get the value of one attribute in a record
//int attrNum is the the position os this attribute, and the result stores in value
RC getAttr(Record *record, Schema *schema, int attrNum, Value **value)
{
	int offset = 0;
	int i = 0;
	//this value store the value of attribute we get in this record
	Value *val = (Value*)malloc(sizeof(Value));
//	char *result; // string we get from record. then we should convert this string to the right data type.
	char *begin = record -> data;
	//caculate the offset of this attribute in this record
	//this part is similar with the getRecordSize()
	offset += (attrNum +1); // add empty spaces to the offset.
	//add the length of attributes to offset
	for(i = 0; i < attrNum; i++)
	{
		switch( schema -> dataTypes[i])
        {
            case DT_INT:
                offset += sizeof(int);
                break;
            case DT_FLOAT:
                offset += sizeof(float);
                break;
            case DT_BOOL:
                offset += sizeof(bool);
                break;
            case DT_STRING:
                offset += schema -> typeLength[i];
                break;
            default:
                break;
        }
	}
	//offset += record -> data;
	
	// get the value according to the dataTypes in schema
	switch(schema -> dataTypes[i])
	{
		case DT_INT:
		  val -> dt = DT_INT;
		  memcpy(&(val->v.intV),record->data + offset,sizeof(int));
		  break;
		case DT_FLOAT:
		  val -> dt = DT_FLOAT;
		  memcpy(&(val->v.floatV),record->data+ offset,sizeof(float));
		  break;
		case DT_BOOL:
		  val -> dt = DT_BOOL;
		  memcpy(&(val->v.boolV),record->data+ offset,sizeof(bool));
		  break;
		case DT_STRING:
		  val -> dt = DT_STRING;
		  int size = schema -> typeLength[i];
          char *temp=malloc(size+1);
          strncpy(temp, record->data+ offset, size); 
          temp[size]='\0';
          val->v.stringV=temp;
		  break;
		
	}
	*value = val;
	free(val);
	return RC_OK;
	
}

