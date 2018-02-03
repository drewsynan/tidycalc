#!/usr/bin/env node
/*

	~* Create an index for a tidy csv table *~
	Takes a flat, tidy csv file and creates an indexed,
	trie-ish JSON object.

	{
		data: [ hashed data ],
		index: { 
			$: [ pointers to rows in the dataset ],
			$$: [ available (hashed) variable names, or variable values ]
			varName: { // sub index object
				$: [ pointers to rows having variable varName ]
				$$: [ list of variable values for varName ]
				varValue: {
					// sub index
				}
			}
			...
		},
		hash: { value -> hash lookup object },
		unhash: { hash -> value lookup object },
		slim: boolean (full or truncated index; truncated means query order matters)
	}

*/

var s3 = require("s3-oo");
var d3 = require('d3');
var fs = require('fs');

function index() {
	var n = object();
	n.$ = [];
	n.$$ = [];

	return n;
}

function identityHash() {
	var symbols = object();
	var index = symbols;

	var hash = function(value) {
		symbols[value] = value;
		return symbols[value];
	}

	hash.hash = function() {
		return symbols;
	}

	hash.unhash = function () {
		return index;
	}

	return hash;
}

function serialHash() {
	var serial = 0;
	var symbols = object();
	var index = object();

	var hash = function(value) {
		if(!symbols[value]) {
			serial++;
			symbols[value] = Number(serial).toString(36);
			index[Number(serial).toString(36)] = value;
		}
		return symbols[value];
	}

	hash.hash = function() {
		return symbols;
	};

	hash.unhash = function() {
		return index;
	};

	return hash;
}

function tuple(data, keys) {
	var t = {data: clone.object(data), keys: clone.array(keys)};
	s3.classOf(t, s3.classOf(t).concat("tuple"));
	return t;
};

var remove = s3.generic(function remove(x, key) {
	return this.useMethod('remove', x, key);
});

remove.tuple = function(tup, key) {
	var _d = clone.object(data.tuple(tup));
	var _k = clone.array(keys.tuple(tup)).filter(function(k){ return k !== key; });

	return tuple(_d, _k);
}

var keys = s3.generic(function keys(x) {
	return this.useMethod('keys', x);
})
keys.tuple = function(tup) {
	return tup.keys;
}
keys.object = Object.keys;


var data = s3.generic(function data(x) {
	return this.useMethod('data', x);
});

data.tuple = function(tup) {
	return tup.data;
}

var clone = s3.generic(function clone(x) {
	return this.useMethod('clone', x);
});
clone.object = function(o) {
	return Object.assign(object(), o);
}
clone.array = function(a) {
	return [].concat(a);
}


function object() { return Object.create(null); }
function set(x) { return new Set(x); }

function processTuple(idx, tup, id, slim) {
	var _data = clone(data(tup));
	var keyNames = clone(keys(tup));

	if(!keyNames.length) {
		// base case
		var oldIdx = set((idx || index()).$);
		var updated = index();
		updated.$ = Array.from(oldIdx.add(id));

		return updated;
	}

	if (slim) { // don't do a full index, just look at the first key
		keyNames = [keyNames[0]];
	}

	idx.$ = idx.$ || [];
	idx.$$ = idx.$$ || [];

	idx.$ = Array.from(set(idx.$).add(id));

	// record variable names
	idx.$$ = Array.from(set(idx.$$.concat(keyNames.map(function(key){ return hash(key) }))));


	keyNames.forEach(function(key){
		var name = hash(key);
		var value = hash(_data[key]);

		if(idx[name] === undefined) { 
			idx[name] = index();
		}

		// record variable values
		var updatedValues = set(idx[name].$$).add(value);
		idx[name].$$ = Array.from(updatedValues);

		var updatedIds = set(idx[name].$).add(id);
		idx[name].$ = Array.from(updatedIds);

		if(idx[name][value] === undefined) {
			idx[name][value] = index() // empty sub-index
		}

		idx[name][value] = processTuple(idx[name][value], remove(tup, key), id, slim);
	});

	return idx;
}

function loadCsv(fname, cols, slim) {
	var columns = "all variables";
	if(cols) {
		columns = "variables " + cols.join(", ");
	}
	console.log("Index " + fname + " on " + columns + "\n");

	var buff = fs.readFileSync(fname, 'utf8');
	var idx = index();

	var data = d3.csvParse(buff, function(d, i) { 
		console.log("~> indexing row: " + i);

		var _d = object();
		var ks = keys.object(d);
		ks.forEach(function(k){
			_d[hash(k)] = hash(d[k]);
		});

		var _data = object();
		var _cols;
		if(cols) {
			_cols = cols;
			cols.forEach(function(col){
				if(!d[col]) {
					throw new Error("Column `" + col + "` does not appear to exist in " + fname);
				}
				_data[col] = d[col];
			});
		} else {
			_cols = ks;
			_data = Object.assign(_data, d); // index on all columns if none are given (yikes exponential growth)
		}

		processTuple(idx, tuple(_data, _cols), i, slim);
		return _d;
	});

	return {
		data: data,
		index: idx,
		hash: hash.hash(),
		unhash: hash.unhash(),
		indexOrder: !!slim? cols : undefined
	};
}

function saveData(data, filename, stringify) {
	console.log("\n~> Writing to " + filename);

	stringify = !!stringify || (stringify === undefined); // no argument defaults to true
	var d;
	if(stringify) {
		d = JSON.stringify(data);
	} else {
		d = data;
	}

	fs.writeFileSync(filename,d);
}

function main() {
	console.log("~~~~~~~~~~~~~~~~");
	console.log("\\ TIDY INDEXER /");
	console.log(" ~~~~~~~~~~~~~~\n");

	if(process.argv.length === 2) {
		var fname = "make-index.js" // todo: split __filename on os separator

		console.log("\nUSAGE:");
		console.log("node " + fname + " [column_names] --csv=source.csv --out=out.js \n[--moduleName=Name] [--noHash] [--slim] [--angular2]");
		console.log("If no column names are given all columns will be included in the index.");
		return 1;
	}

	var args = parseArgs(process.argv.slice(2));
	var inFile = args.csv || "source.csv";
	var moduleName = args.moduleName;
	var isAngular2 = args.angular2 !== undefined;
	var isEs6 = args.es6 !== undefined;

	var outFile;
	if(args.out) {
		outFile = args.out;
	} else {
		if(moduleName) {
			if (isAngular2) {
				outFile = "ng-" + moduleName.toLowerCase() +".js";
			} else {
				outFile = moduleName + ".js";
			}
		} else {
			outFile = "out.json";
		}
	}

	var cols = (args._.length > 0)? args._ : undefined;

	var slimIndex = !!args.slim;

	if(!!args.noHash) {
		hash = identityHash();
	}

	function makeWindowModule(varName, data){
		var outString = `;(function(d){
						       return d.${varName} = ${JSON.stringify(data)};
						   })(window);`;
		return outString;
	}

	function makeAngular2Module(varName, data) {
		var outString = `angular.module('${varName}', ['ngTidycalc'])
			.provider('${varName}', function(){
				this.$get = function(tidycalc) {
					return tidycalc(${JSON.stringify(data)});
				};
			});`;

		return outString;
	}

	function makeEs6Module(varName, data) {
		var outString = `export default ${JSON.stringify(data)};`;
		return outString;
	}

	function makeJson(data) {
		return JSON.stringify(data);
	}

	var processed = loadCsv(inFile, cols, slimIndex);
	var outString;

	if(moduleName) {
		if (isAngular2) {
			outString = makeAngular2Module(moduleName, processed);
		} else if (isEs6) {
			outString = makeEs6Module(moduleName, processed);
		} else {
			outString = makeWindowModule(moduleName, processed);
		}
	} else {
		outString = makeJson(processed);
	}

	saveData(outString, outFile, false);
}

try {
	var parseArgs = require('minimist');
	var hash = serialHash();

	main();
} catch(err) {
	console.error("\n~~~~~~~~~~~~~~~~~~\nThere was an error\n~~~~~~~~~~~~~~~~~~");
	console.error("~> " + err.message);
	console.error(err);
}