########################################################################
# $Id: $
########################################################################

"""
This class a wrapper around elasticsearch-py. It is used to query
Elasticsearch database.

"""

__RCSID__ = "$Id$"

from DIRAC                      import gLogger, S_OK, S_ERROR
from elasticsearch              import Elasticsearch
from elasticsearch.exceptions   import ConnectionError
from datetime                   import datetime

class ElasticSearchDB( object ):
  
  """
  .. class:: ElasticSearchDB

  :param str url: the url to the database for example: el.cern.ch:9200
  :param str gDebugFile: is used to save the debug information to a file
  """
  
  __url = ""
  ########################################################################
  def __init__( self, host, port, debug = False ):
    """ c'tor
    :param self: self reference
    :param str host: name of the database for example: MonitoringDB
    :param str port: The full name of the database for example: 'Monitoring/MonitoringDB'
    :param bool debug: save the debug information to a file   
    """
    global gDebugFile
    
    if 'log' not in dir( self ):
      self.log = gLogger.getSubLogger( 'ElasticSearch' )
    self.logger = self.log
    
    self.__url = "http://%s:%s" % ( host, port )
        
    if debug:
      try:
        gDebugFile = open( "%s.debug.log" % self.__dbName, "w" )
      except IOError as e:
        self.log.error( e )
      
    self.__client = Elasticsearch( self.__url )
    self.__tryToConnect()
  
  ########################################################################  
  def query( self, index, query = None ):
    """It executes a query and it returns the result
    query is a dictionary. More info: search for elasticsearch dsl
    
    :param self: self reference
    :param index: Name of the index to perform the query on
    :param dict query: It is the query in ElasticSerach DSL language
     
    """
    try:
      return S_OK( self.__client.search( index = index, body = query ) )
    except Exception as e:
      return S_ERROR( e )
  
  ########################################################################
  def __tryToConnect( self ):
    """Before we use the database we try to connect and retrive the cluster name
    
    :param self: self reference
         
    """
    try:
      if self.__client.ping():
        result = self.__client.info()
        self.setClusterName ( result.get( "cluster_name", " " ) )
        self.log.info( "Database info", result )
        self._connected = True
      else:
        self.log.error( "Cannot connect to the database!" )
    except ConnectionError as e:
      self.log.error( e )
      self._connected = False 

  ########################################################################
  def getIndexes( self ):
    """
    It returns the available indexes...
    """
    return [ index for index in self.__client.indices.get_aliases() ]
  
  ########################################################################
  def getDocTypes( self, indexes ):
    try:
      result = self.__client.indices.get_mapping( indexes )
    except Exception as e:
      print e
    doctype = ''
    for i in result:
      doctype = result[i]['mappings'].keys()[0]
      break
    return doctype 
  
  ########################################################################
  def checkIndex( self, indexName ):
    """
    it checks the existance of an index
    :param str indexName: the name of the index
    """
    try:
      return S_OK( self.__client.indices.exists( indexName ) )
    except Exception as e:
      return S_ERROR( e )
  
  ########################################################################
  def createFullIndexName( self, indexName ):
    """
    Given an index perfix we create the actual index name.
    :param str indexName: it is the name of the index
    """
    today = datetime.today().strftime( "%Y-%m-%d" )
    return "%s-%s" % ( indexName, today )
  
  def createIndex( self, indexPrefix, mapping ):
    """
    :param str indexPrefix: it is the index name. 
    :param dict mapping: the configuration of the index.
    
    """
    result = S_OK( "Index created" )
    fullIndex = self.createFullIndexName( indexPrefix )  # we have to create the an index in each day...
    try:
      self.log.info( "Create index: ", fullIndex + str( mapping ) )
      self.__client.indices.create( fullIndex, body = mapping )
    except Exception as e:
      result = S_ERROR( e )
    return result
    
  def addToIndex( self, index, docType, mapping ):
    """
    :param str index: index to store the document in
    :param dict mapping: contents to be stored in the index
    """
    try:
      self.__client.create( index = index, doc_type = docType, body = mapping, refresh = True )
    except Exception as e:
      return S_ERROR( e )
    return S_OK( 'Document added' )

  def getDocCount( self, indexName ):
    """
    Retrieve the number of documents in an index
    :param indexName: name of the index
    """
    try:
      result = self.__client.indices.stats( index = indexName, metric = [ 'docs' ] )
      nDocs = result[ 'indices' ][ indexName ][ 'total' ][ 'docs' ][ 'count' ]
    except Exception as e:
      return S_ERROR( e )
    return S_OK( nDocs )

  def deleteIndexes( self, indexList ):
    """
    Deletes indexes from ElasticSearch
    :param indexList: list of indexes to be deleted
    """
    try:
      self.__client.indices.delete( indexList )
    except Exception as e:
      return S_ERROR( e )

    return S_OK( 'Index(es) deleted' )

  def deleteDocuments( self, query, index = '_all' ):
    """
    Delete documents from the given indexes
    :param query: ElasticSearch DSL language query that documents to be deleted should match
    :param index: Index to delete from. If not provided, it will delete from all indexes
    """
    search = self.__client.search( index = index, body = query, search_type = 'scan', scroll = '5m' )

    while True:
      try:
        # Git the next page of results.
        scroll = self.__client.scroll( scroll_id = search[ '_scroll_id' ], scroll = '5m' )
      # Since scroll throws an error catch it and break the loop.
      except Exception as e:
        break
      # We have results initialize the bulk variable.
      bulk = ''
      for result in scroll[ 'hits' ][ 'hits' ]:
        bulk = bulk + '{ "delete" : { "_index" : "' + str(result['_index']) + '", "_type" : "' + str(result['_type']) + '", "_id" : "' + str(result['_id']) + '" } }\n'
      # Finally do the deleting.
      if bulk:
        try:
          self.__client.bulk( body = bulk )
        except Exception as e:
          return S_ERROR( e )
      else:
        return S_ERROR( 'Nothing to delete' )

    return S_OK( 'Documents deleted' )
