import os, json, UserDict, requests, uuid

from datetime import datetime

HOST = "localhost:9200/"
DB = "gtrraw"

class DomainObject(UserDict.IterableUserDict):
    __type__ = None # set the type on the model that inherits this

    def __init__(self, **kwargs):
        if '_source' in kwargs:
            self.data = dict(kwargs['_source'])
            self.meta = dict(kwargs)
            del self.meta['_source']
        else:
            self.data = dict(kwargs)
    
    @classmethod
    def target(cls, rid=None, feature=None, db_only=False):
        t = HOST
        if not t.startswith("http://") and not t.startswith("https://"):
            t = "http://" + t
        t = t.rstrip("/") + '/' + DB
        if cls.__type__ is not None and not db_only:
            t += "/" + cls.__type__ + '/'
        if rid is not None:
            t += rid
        elif feature is not None:
            t += feature
        return t
    
    @classmethod
    def type_mapping(cls, mapping_definition):
        t = cls.target(feature="_mapping")
        requests.put(t, data=json.dumps(mapping_definition))
    
    @classmethod
    def db_mapping(cls, mapping_definition):
        t = cls.target(feature="_mapping", db_only=True)
        requests.put(t, data=json.dumps(mapping))
        
    @classmethod
    def term(cls, field, value, one_answer=False, raw=False):
        query = {
            "query" : {
                "term" : {field : value}
            }
        }
        result = cls.query(q=query)
        
        if not raw:
            objects = [i.get("_source", {}) for i in result.get('hits', {}).get('hits', [])]
            if one_answer:
                return objects[0]
            return objects
        else:
            return result
    
    @classmethod
    def query(cls, recid='', endpoint='_search', q='', 
                terms=None, facets=None, raw=True, from_record=None, 
                result_size=None, **kwargs):
        '''Perform a query on backend.

        :param recid: needed if endpoint is about a record, e.g. mlt
        :param endpoint: default is _search, but could be _mapping, _mlt, _flt etc.
        :param q: maps to query_string parameter if string, or query dict if dict.
        :param terms: dictionary of terms to filter on. values should be lists. 
        :param facets: dict of facets to return from the query.
        :param kwargs: any keyword args as per
            http://www.elasticsearch.org/guide/reference/api/search/uri-request.html
        '''
        if recid and not recid.endswith('/'): recid += '/'
        if isinstance(q,dict):
            query = q
        elif q:
            query = {'query': {'query_string': { 'query': q }}}
        else:
            query = {'query': {'match_all': {}}}

        if facets:
            if 'facets' not in query:
                query['facets'] = {}
            for k, v in facets.items():
                query['facets'][k] = {"terms":v}

        if terms:
            boolean = {'must': [] }
            for term in terms:
                if not isinstance(terms[term],list): terms[term] = [terms[term]]
                for val in terms[term]:
                    obj = {'term': {}}
                    obj['term'][ term ] = val
                    boolean['must'].append(obj)
            if q and not isinstance(q,dict):
                boolean['must'].append( {'query_string': { 'query': q } } )
            elif q and 'query' in q:
                boolean['must'].append( query['query'] )
            query['query'] = {'bool': boolean}
        
        if from_record is not None:
            query['from'] = from_record
        
        if result_size is not None:
            query['size'] = result_size
        
        for k,v in kwargs.items():
            query[k] = v

        # FIXME: not sure why this is here
        if endpoint in ['_mapping']:
            r = requests.get(cls.target() + recid + endpoint)
            return r.json()
        
        # FIXME: can probably deprecate the endpoint
        r = requests.post(cls.target(recid) + endpoint, data=json.dumps(query))
        result = r.json()
        
        if not raw:
            return [i.get("_source", {}) for i in result.get('hits', {}).get('hits', [])]
        return result
    
    @classmethod
    def _makeid(self):
        '''Create a new id for data object
        overwrite this in specific model types if required'''
        return uuid.uuid4().hex
    
    def save(self):
        if 'id' in self.data:
            id_ = self.data['id'].strip()
        else:
            id_ = self._makeid()
            self.data['id'] = id_
        
        self.data['last_updated'] = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

        if 'created_date' not in self.data:
            self.data['created_date'] = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        
        t = self.target(rid=self.data['id'])
        r = requests.post(t, data=json.dumps(self.data))
    
    @classmethod
    def bulk(cls, records, refresh=False):
        data = ''
        for r in records:
            if "id" not in r:
                r['id'] = cls._makeid()
            data += json.dumps( {'index':{'_id':r['id']}} ) + '\n'
            data += json.dumps( r ) + '\n'
        r = requests.post(cls.target(feature="_bulk"), data=data)
        if refresh:
            cls.refresh()
        return r.json()
    
    @classmethod
    def refresh(cls):
        r = requests.post(cls.target(feature="_refresh"))
        return r.json()
