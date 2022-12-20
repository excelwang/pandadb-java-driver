// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: query.proto

package org.grapheco.pandadb.network;

public final class Query {
  private Query() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }
  public interface QueryRequestOrBuilder extends
      // @@protoc_insertion_point(interface_extends:org.grapheco.pandadb.network.QueryRequest)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>optional string statement = 1;</code>
     */
    String getStatement();
    /**
     * <code>optional string statement = 1;</code>
     */
    com.google.protobuf.ByteString
        getStatementBytes();
  }
  /**
   * Protobuf type {@code org.grapheco.pandadb.network.QueryRequest}
   */
  public  static final class QueryRequest extends
      com.google.protobuf.GeneratedMessageV3 implements
      // @@protoc_insertion_point(message_implements:org.grapheco.pandadb.network.QueryRequest)
      QueryRequestOrBuilder {
    // Use QueryRequest.newBuilder() to construct.
    private QueryRequest(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }
    private QueryRequest() {
      statement_ = "";
    }

    @Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return com.google.protobuf.UnknownFieldSet.getDefaultInstance();
    }
    private QueryRequest(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      this();
      int mutable_bitField0_ = 0;
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!input.skipField(tag)) {
                done = true;
              }
              break;
            }
            case 10: {
              String s = input.readStringRequireUtf8();

              statement_ = s;
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e).setUnfinishedMessage(this);
      } finally {
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return Query.internal_static_org_grapheco_pandadb_network_QueryRequest_descriptor;
    }

    protected FieldAccessorTable
        internalGetFieldAccessorTable() {
      return Query.internal_static_org_grapheco_pandadb_network_QueryRequest_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              QueryRequest.class, Builder.class);
    }

    public static final int STATEMENT_FIELD_NUMBER = 1;
    private volatile Object statement_;
    /**
     * <code>optional string statement = 1;</code>
     */
    public String getStatement() {
      Object ref = statement_;
      if (ref instanceof String) {
        return (String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        String s = bs.toStringUtf8();
        statement_ = s;
        return s;
      }
    }
    /**
     * <code>optional string statement = 1;</code>
     */
    public com.google.protobuf.ByteString
        getStatementBytes() {
      Object ref = statement_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (String) ref);
        statement_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      if (!getStatementBytes().isEmpty()) {
        com.google.protobuf.GeneratedMessageV3.writeString(output, 1, statement_);
      }
    }

    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      if (!getStatementBytes().isEmpty()) {
        size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, statement_);
      }
      memoizedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @Override
    public boolean equals(final Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof QueryRequest)) {
        return super.equals(obj);
      }
      QueryRequest other = (QueryRequest) obj;

      boolean result = true;
      result = result && getStatement()
          .equals(other.getStatement());
      return result;
    }

    @Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptorForType().hashCode();
      hash = (37 * hash) + STATEMENT_FIELD_NUMBER;
      hash = (53 * hash) + getStatement().hashCode();
      hash = (29 * hash) + unknownFields.hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static QueryRequest parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static QueryRequest parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static QueryRequest parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static QueryRequest parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static QueryRequest parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static QueryRequest parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }
    public static QueryRequest parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input);
    }
    public static QueryRequest parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static QueryRequest parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static QueryRequest parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }

    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder() {
      return DEFAULT_INSTANCE.toBuilder();
    }
    public static Builder newBuilder(QueryRequest prototype) {
      return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() {
      return this == DEFAULT_INSTANCE
          ? new Builder() : new Builder().mergeFrom(this);
    }

    @Override
    protected Builder newBuilderForType(
        BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code org.grapheco.pandadb.network.QueryRequest}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:org.grapheco.pandadb.network.QueryRequest)
        QueryRequestOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return Query.internal_static_org_grapheco_pandadb_network_QueryRequest_descriptor;
      }

      protected FieldAccessorTable
          internalGetFieldAccessorTable() {
        return Query.internal_static_org_grapheco_pandadb_network_QueryRequest_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                QueryRequest.class, Builder.class);
      }

      // Construct using org.grapheco.pandadb.network.Query.QueryRequest.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessageV3
                .alwaysUseFieldBuilders) {
        }
      }
      public Builder clear() {
        super.clear();
        statement_ = "";

        return this;
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return Query.internal_static_org_grapheco_pandadb_network_QueryRequest_descriptor;
      }

      public QueryRequest getDefaultInstanceForType() {
        return QueryRequest.getDefaultInstance();
      }

      public QueryRequest build() {
        QueryRequest result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public QueryRequest buildPartial() {
        QueryRequest result = new QueryRequest(this);
        result.statement_ = statement_;
        onBuilt();
        return result;
      }

      public Builder clone() {
        return (Builder) super.clone();
      }
      public Builder setField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          Object value) {
        return (Builder) super.setField(field, value);
      }
      public Builder clearField(
          com.google.protobuf.Descriptors.FieldDescriptor field) {
        return (Builder) super.clearField(field);
      }
      public Builder clearOneof(
          com.google.protobuf.Descriptors.OneofDescriptor oneof) {
        return (Builder) super.clearOneof(oneof);
      }
      public Builder setRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          int index, Object value) {
        return (Builder) super.setRepeatedField(field, index, value);
      }
      public Builder addRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          Object value) {
        return (Builder) super.addRepeatedField(field, value);
      }
      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof QueryRequest) {
          return mergeFrom((QueryRequest)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(QueryRequest other) {
        if (other == QueryRequest.getDefaultInstance()) return this;
        if (!other.getStatement().isEmpty()) {
          statement_ = other.statement_;
          onChanged();
        }
        onChanged();
        return this;
      }

      public final boolean isInitialized() {
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        QueryRequest parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (QueryRequest) e.getUnfinishedMessage();
          throw e.unwrapIOException();
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }

      private Object statement_ = "";
      /**
       * <code>optional string statement = 1;</code>
       */
      public String getStatement() {
        Object ref = statement_;
        if (!(ref instanceof String)) {
          com.google.protobuf.ByteString bs =
              (com.google.protobuf.ByteString) ref;
          String s = bs.toStringUtf8();
          statement_ = s;
          return s;
        } else {
          return (String) ref;
        }
      }
      /**
       * <code>optional string statement = 1;</code>
       */
      public com.google.protobuf.ByteString
          getStatementBytes() {
        Object ref = statement_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (String) ref);
          statement_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>optional string statement = 1;</code>
       */
      public Builder setStatement(
          String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  
        statement_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional string statement = 1;</code>
       */
      public Builder clearStatement() {
        
        statement_ = getDefaultInstance().getStatement();
        onChanged();
        return this;
      }
      /**
       * <code>optional string statement = 1;</code>
       */
      public Builder setStatementBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  checkByteStringIsUtf8(value);
        
        statement_ = value;
        onChanged();
        return this;
      }
      public final Builder setUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return this;
      }

      public final Builder mergeUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return this;
      }


      // @@protoc_insertion_point(builder_scope:org.grapheco.pandadb.network.QueryRequest)
    }

    // @@protoc_insertion_point(class_scope:org.grapheco.pandadb.network.QueryRequest)
    private static final QueryRequest DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new QueryRequest();
    }

    public static QueryRequest getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    private static final com.google.protobuf.Parser<QueryRequest>
        PARSER = new com.google.protobuf.AbstractParser<QueryRequest>() {
      public QueryRequest parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
          return new QueryRequest(input, extensionRegistry);
      }
    };

    public static com.google.protobuf.Parser<QueryRequest> parser() {
      return PARSER;
    }

    @Override
    public com.google.protobuf.Parser<QueryRequest> getParserForType() {
      return PARSER;
    }

    public QueryRequest getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  public interface QueryResponseOrBuilder extends
      // @@protoc_insertion_point(interface_extends:org.grapheco.pandadb.network.QueryResponse)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>optional bytes resultInBytes = 1;</code>
     */
    com.google.protobuf.ByteString getResultInBytes();
  }
  /**
   * Protobuf type {@code org.grapheco.pandadb.network.QueryResponse}
   */
  public  static final class QueryResponse extends
      com.google.protobuf.GeneratedMessageV3 implements
      // @@protoc_insertion_point(message_implements:org.grapheco.pandadb.network.QueryResponse)
      QueryResponseOrBuilder {
    // Use QueryResponse.newBuilder() to construct.
    private QueryResponse(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }
    private QueryResponse() {
      resultInBytes_ = com.google.protobuf.ByteString.EMPTY;
    }

    @Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return com.google.protobuf.UnknownFieldSet.getDefaultInstance();
    }
    private QueryResponse(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      this();
      int mutable_bitField0_ = 0;
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!input.skipField(tag)) {
                done = true;
              }
              break;
            }
            case 10: {

              resultInBytes_ = input.readBytes();
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e).setUnfinishedMessage(this);
      } finally {
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return Query.internal_static_org_grapheco_pandadb_network_QueryResponse_descriptor;
    }

    protected FieldAccessorTable
        internalGetFieldAccessorTable() {
      return Query.internal_static_org_grapheco_pandadb_network_QueryResponse_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              QueryResponse.class, Builder.class);
    }

    public static final int RESULTINBYTES_FIELD_NUMBER = 1;
    private com.google.protobuf.ByteString resultInBytes_;
    /**
     * <code>optional bytes resultInBytes = 1;</code>
     */
    public com.google.protobuf.ByteString getResultInBytes() {
      return resultInBytes_;
    }

    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      if (!resultInBytes_.isEmpty()) {
        output.writeBytes(1, resultInBytes_);
      }
    }

    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      if (!resultInBytes_.isEmpty()) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(1, resultInBytes_);
      }
      memoizedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @Override
    public boolean equals(final Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof QueryResponse)) {
        return super.equals(obj);
      }
      QueryResponse other = (QueryResponse) obj;

      boolean result = true;
      result = result && getResultInBytes()
          .equals(other.getResultInBytes());
      return result;
    }

    @Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptorForType().hashCode();
      hash = (37 * hash) + RESULTINBYTES_FIELD_NUMBER;
      hash = (53 * hash) + getResultInBytes().hashCode();
      hash = (29 * hash) + unknownFields.hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static QueryResponse parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static QueryResponse parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static QueryResponse parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static QueryResponse parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static QueryResponse parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static QueryResponse parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }
    public static QueryResponse parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input);
    }
    public static QueryResponse parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static QueryResponse parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static QueryResponse parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }

    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder() {
      return DEFAULT_INSTANCE.toBuilder();
    }
    public static Builder newBuilder(QueryResponse prototype) {
      return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() {
      return this == DEFAULT_INSTANCE
          ? new Builder() : new Builder().mergeFrom(this);
    }

    @Override
    protected Builder newBuilderForType(
        BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code org.grapheco.pandadb.network.QueryResponse}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:org.grapheco.pandadb.network.QueryResponse)
        QueryResponseOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return Query.internal_static_org_grapheco_pandadb_network_QueryResponse_descriptor;
      }

      protected FieldAccessorTable
          internalGetFieldAccessorTable() {
        return Query.internal_static_org_grapheco_pandadb_network_QueryResponse_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                QueryResponse.class, Builder.class);
      }

      // Construct using org.grapheco.pandadb.network.Query.QueryResponse.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessageV3
                .alwaysUseFieldBuilders) {
        }
      }
      public Builder clear() {
        super.clear();
        resultInBytes_ = com.google.protobuf.ByteString.EMPTY;

        return this;
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return Query.internal_static_org_grapheco_pandadb_network_QueryResponse_descriptor;
      }

      public QueryResponse getDefaultInstanceForType() {
        return QueryResponse.getDefaultInstance();
      }

      public QueryResponse build() {
        QueryResponse result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public QueryResponse buildPartial() {
        QueryResponse result = new QueryResponse(this);
        result.resultInBytes_ = resultInBytes_;
        onBuilt();
        return result;
      }

      public Builder clone() {
        return (Builder) super.clone();
      }
      public Builder setField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          Object value) {
        return (Builder) super.setField(field, value);
      }
      public Builder clearField(
          com.google.protobuf.Descriptors.FieldDescriptor field) {
        return (Builder) super.clearField(field);
      }
      public Builder clearOneof(
          com.google.protobuf.Descriptors.OneofDescriptor oneof) {
        return (Builder) super.clearOneof(oneof);
      }
      public Builder setRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          int index, Object value) {
        return (Builder) super.setRepeatedField(field, index, value);
      }
      public Builder addRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          Object value) {
        return (Builder) super.addRepeatedField(field, value);
      }
      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof QueryResponse) {
          return mergeFrom((QueryResponse)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(QueryResponse other) {
        if (other == QueryResponse.getDefaultInstance()) return this;
        if (other.getResultInBytes() != com.google.protobuf.ByteString.EMPTY) {
          setResultInBytes(other.getResultInBytes());
        }
        onChanged();
        return this;
      }

      public final boolean isInitialized() {
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        QueryResponse parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (QueryResponse) e.getUnfinishedMessage();
          throw e.unwrapIOException();
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }

      private com.google.protobuf.ByteString resultInBytes_ = com.google.protobuf.ByteString.EMPTY;
      /**
       * <code>optional bytes resultInBytes = 1;</code>
       */
      public com.google.protobuf.ByteString getResultInBytes() {
        return resultInBytes_;
      }
      /**
       * <code>optional bytes resultInBytes = 1;</code>
       */
      public Builder setResultInBytes(com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  
        resultInBytes_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional bytes resultInBytes = 1;</code>
       */
      public Builder clearResultInBytes() {
        
        resultInBytes_ = getDefaultInstance().getResultInBytes();
        onChanged();
        return this;
      }
      public final Builder setUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return this;
      }

      public final Builder mergeUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return this;
      }


      // @@protoc_insertion_point(builder_scope:org.grapheco.pandadb.network.QueryResponse)
    }

    // @@protoc_insertion_point(class_scope:org.grapheco.pandadb.network.QueryResponse)
    private static final QueryResponse DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new QueryResponse();
    }

    public static QueryResponse getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    private static final com.google.protobuf.Parser<QueryResponse>
        PARSER = new com.google.protobuf.AbstractParser<QueryResponse>() {
      public QueryResponse parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
          return new QueryResponse(input, extensionRegistry);
      }
    };

    public static com.google.protobuf.Parser<QueryResponse> parser() {
      return PARSER;
    }

    @Override
    public com.google.protobuf.Parser<QueryResponse> getParserForType() {
      return PARSER;
    }

    public QueryResponse getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_org_grapheco_pandadb_network_QueryRequest_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_org_grapheco_pandadb_network_QueryRequest_fieldAccessorTable;
  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_org_grapheco_pandadb_network_QueryResponse_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_org_grapheco_pandadb_network_QueryResponse_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    String[] descriptorData = {
      "\n\013query.proto\022\034org.grapheco.pandadb.netw" +
      "ork\"!\n\014QueryRequest\022\021\n\tstatement\030\001 \001(\t\"&" +
      "\n\rQueryResponse\022\025\n\rresultInBytes\030\001 \001(\0142y" +
      "\n\021PandaQueryService\022d\n\005Query\022*.org.graph" +
      "eco.pandadb.network.QueryRequest\032+.org.g" +
      "rapheco.pandadb.network.QueryResponse\"\0000" +
      "\001B\036\n\034org.grapheco.pandadb.networkb\006proto" +
      "3"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
        new com.google.protobuf.Descriptors.FileDescriptor.    InternalDescriptorAssigner() {
          public com.google.protobuf.ExtensionRegistry assignDescriptors(
              com.google.protobuf.Descriptors.FileDescriptor root) {
            descriptor = root;
            return null;
          }
        };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        }, assigner);
    internal_static_org_grapheco_pandadb_network_QueryRequest_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_org_grapheco_pandadb_network_QueryRequest_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_org_grapheco_pandadb_network_QueryRequest_descriptor,
        new String[] { "Statement", });
    internal_static_org_grapheco_pandadb_network_QueryResponse_descriptor =
      getDescriptor().getMessageTypes().get(1);
    internal_static_org_grapheco_pandadb_network_QueryResponse_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_org_grapheco_pandadb_network_QueryResponse_descriptor,
        new String[] { "ResultInBytes", });
  }

  // @@protoc_insertion_point(outer_class_scope)
}