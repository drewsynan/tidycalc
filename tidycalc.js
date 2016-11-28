;(function(){
function tidycalc(indexedData) {
	indexedData = Object.freeze(indexedData);

	function hash(value) 	{ return indexedData.hash[value]; 	}
	function unhash(value)	{ return indexedData.unhash[value];	}
	function get(data, key)	{ return data[key];					}
	function getKeys(data)	{ return Object.keys(data);			}


	var extractor = function(query) {
		try {
			if(!Array.isArray(query) && typeof query === "object") {
				if(get(indexedData, 'indexOrder') !== undefined) {
					throw new Error("Cannot use object notation for queries on slimmed (ordered) index. Use array notation instead.");
				}

				// Object.keys(query).reduce(function(list, key){
				// 	return list.concat(key).concat(query[key]);
				// }, []);

				var list = [];
				var keys = Object.keys(query);
				for(var i=0; i<keys.length; i++) {
					list.push(keys[i]);
					list.push(query[keys[i]]);
				}

				query = list;
			} else if(!Array.isArray(query)) {
				throw new Error("Invalid query");
			}

			// var ptrs = query.reduce(function(acc, term){
			// 	var hashed = indexedData.hash[term];
			// 	var subIndex = acc[hashed];
			// 	return subIndex;
			// }, indexedData.index);

			
			var queried = get(indexedData, 'index');
			for(var i=0; i<query.length; i++) {
				var term = query[i];
				var hashed = hash(term);
				queried = get(queried, hashed);
			}

			var ptrs = get(queried, '$');
			var results = [];
			for(var i=0; i<ptrs.length; i++){
				var hashedRow = get(indexedData.data, get(ptrs, i));
				var unhashed = Object.create(null);

				var keys = getKeys(hashedRow);

				for(var j=0; j<keys.length; j++) {
					unhashed[unhash(keys[j])] = unhash(get(hashedRow, keys[j]));
				}

				results.push(unhashed);
			}

			return results;
			// return ptrs.$.map(function(ptr){
			// 	var hashed = indexedData.data[ptr];
			// 	var unhashed = Object.create(null);

			// 	Object.keys(hashed).forEach(function(key){
			// 		unhashed[indexedData.unhash[key]] = indexedData.unhash[hashed[key]];
			// 	});

			// 	return unhashed;
			// });


		} catch(e) {
			console.error(e);
			return [];
		}
	};

	extractor.all = function(query) {
		query = query || [];
		try {
			if(!Array.isArray(query) && typeof query === "object") {
				if(get(indexedData, 'indexOrder') !== undefined) {
					throw new Error("Cannot use object notation for queries on slimmed (ordered) index. Use array notation instead.");
				}
				var list = [];
				var keys = Object.keys(query);

				for(var i=0; i<keys.length; i++) {
					var key = keys[i];
					list.push(key);
					if(query[key] !== null && query[key] !== undefined) {
						list.push(query[key]);
					} else {
						// only one null at a time, skip over the rest of the query
						i = keys.length;
					}
				}
				query = list;

			} else if(!Array.isArray(query)) {
				throw new Error("Invalid query");
			}

			// var hashedNames = query.reduce(function(acc, term){
			// 	var hashed = indexedData.hash[term];
			// 	var subIndex = acc[hashed];
			// 	return subIndex;
			// }, indexedData.index).$$;

			// return hashedNames.map(function(name){ return indexedData.unhash[name]; });
			var subIndex = get(indexedData, 'index');
			for(var i=0; i<query.length; i++) {
				var term = query[i];
				var hashed = hash(term);
				subIndex = get(subIndex, hashed);
			}

			var hashedNames = get(subIndex, '$$');
			var unhashedNames = [];
			for(var i=0; i<hashedNames.length; i++) {
				unhashedNames.push(unhash(hashedNames[i]));
			}
			return unhashedNames;

		} catch(e) {
			return undefined;
		}
	}

	return extractor;
}

typeof module !== "undefined" ? module.exports = tidycalc : window.tidycalc = tidycalc;

}());