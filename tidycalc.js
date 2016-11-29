;(function(){
function tidycalc(indexedData) {
	indexedData = Object.freeze(indexedData);

	function hash(value) 	{ 
		function hash1(v) {
			var hashed = indexedData.hash[v];
			if(hashed === undefined) {
				throw new Error("Could not hash " + v +": not in hash table");
			}
			return hashed;
		}

		// hash an array 
		if(Array.isArray(value)) {
			return value.map(hash1);
		} else if (!Array.isArray(value) && typeof value === "object") {
			var hashed = Object.create(null);
			var keys = getKeys(value);
			for(var i=0; i<keys.length; i++) {
				var key = keys[i];
				hashed[hash1(key)] = hash(value);
			}
			return hashed;
		} else {
			return hash1(value);
		}
	}
	function unhash(value)	{
		// unhash a value
		function unhash1(v) {
			var unhashed = indexedData.unhash[v];
			if(unhashed === undefined) {
				throw new Error("Could not unhash " + v + ": value not found in hash table");
			}
			return unhashed;
		}

		// unhash an array
		if(Array.isArray(value)) {
			return value.map(unhash1);
		
		// unhash an object
		} else if(!Array.isArray(value) && typeof value === "object") {
			var unhashed = Object.create(null);
			getKeys(value).forEach(function(key){
				unhashed[unhash(key)] = unhash(value[key]);
			});

			return unhashed;
		} else {
			return unhash1(value);
		}
	}
	function get(data, key)	{ return data[key];					}
	function getKeys(data)	{ return Object.keys(data);			}
	function unhashRow(rowPtr) {
		var hashedRow = get(get(indexedData, 'data'), rowPtr);
		return unhash(hashedRow);
	}
	function processQuery(query) {
		if(query === undefined) {
			query = [];
		} else if(typeof query !== "object") {
			query = [].concat(query);
		}

		if(!Array.isArray(query) && typeof query === "object") {
			if(get(indexedData, 'indexOrder') !== undefined) {
				throw new Error("Cannot use object notation for queries on slimmed (ordered) index. Use array notation instead.");
			}
			query = Object.keys(query).reduce(function(list, key){
				var term = query[key];
				if(term === undefined || term === null) { // stop processing
					return list.concat(key);
				} else {
					return list.concat(key).concat(term);
				}
			}, []);
		} else if(!Array.isArray(query)) {
			throw new Error("Invalid query");
		}

		return query;
	}

	function extract(rows, varName) {
		function extract1(row) {
			return row[varName];
		}
		rows = [].concat(rows);
		return rows.map(extract1);
	}

	function focusIndex(query) {
		return query.reduce(function(subIndex, term){
				return get(subIndex, hash(term));
			}, get(indexedData, 'index'));
	}

	function select(query, limit) {
		query = processQuery(query);
		limit = limit || 0;

		try {
			var focused = focusIndex(query);
			if(focused === undefined) {
				// no matching rows found
				//console.warn("No matching rows found for " + query);
				return [];
			}
			var ptrs = get(focused, '$');

			if(limit > 0) { ptrs = ptrs.slice(0, limit); }
			results = ptrs.map(unhashRow);

			return results;
		} catch(e) {
			//console.warn(e);
			return [];
		}
	}

	var reader = function reader(){};

	reader.data = unhashRow;
	reader.selectMany = function selectMany(query, extractVar) {
		var results = select(query, 0);
		if(extractVar !== undefined) { results = extract(results, extractVar); }

		return results;
	};

	reader.selectOne = function selectOne(query, extractVar) {
		var results = select(query, 1);

		if(extractVar !== undefined) {
			results = extract(results, extractVar);
		}

		return results[0];
	};

	reader.levels = function levels(query) {
		query = processQuery(query);
		try {
			var focused = focusIndex(query);
			return unhash(get(focused, '$$'));
		} catch(e) {
			console.warn("No level values found for " + query);
			return [];
		}
	}

	return reader;
}

var isNodejs = typeof module !== "undefined";
var isAngular = typeof angular === "object";
var isBrowser = typeof window === "object";

if(isNodejs) {
	module.exports = tidycalc;
} else if(isAngular) {
	angular.module('ngTidycalc', [])
		   .provider('tidycalc', function(){
		   		this.$get = function(){
		   			return tidycalc;
		   		};
		   });
} else {
	window.tidycalc = tidycalc;
}

}());