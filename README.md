[![Build Status](https://travis-ci.org/shove70/database.svg?branch=master)](https://travis-ci.org/shove70/database)
[![GitHub tag](https://img.shields.io/github/tag/shove70/database.svg?maxAge=86400)](https://github.com/shove70/database/releases)
[![Dub downloads](https://img.shields.io/dub/dt/database.svg)](http://code.dlang.org/packages/database)

# database
A lightweight native MySQL/MariaDB & PostgreSQL driver written in D.

The goal is a native driver that re-uses the same buffers and the stack as much as possible,
avoiding unnecessary allocations and work for the garbage collector

Native. No link, No harm :)

## example
```d
import std.datetime;
import std.stdio;
import mysql;

void main() {
	auto conn = new Connection("host=127.0.0.1;user=root;pwd=pwd;db=test");
	// auto conn = new Connection("127.0.0.1", "root", "pwd", "test", 3306);

	// change database
	conn.use("mewmew");

	// simple insert statement
	conn.execute("insert into users (name, email) values (?, ?)", "frank", "thetank@cowabanga.com");
	auto id = conn.lastInsertId;

	struct User {
		string name;
		string email;
	}

	// simple select statement
	User[] users;
	conn.execute("select name, email from users where id > ?", 13, (MySQLRow row) {
		users ~= row.toStruct!User;
	});


	// simple select statement
	string[string][] rows;
	conn.execute("select name, email from users where id > ?", 13, (MySQLRow row) {
		rows ~= row.toAA();
	});

	foreach(row; rows) {
		writeln(row["name"], row["email"]);
	}


	// batch inserter - inserts in packets of 128k bytes
	auto insert = inserter(conn, "users_copy", "name", "email");
	foreach(user; users)
		insert.row(user.name, user.email);
	insert.flush;


	// re-usable prepared statements
	auto upd = conn.prepare("update users set sequence = ?, login_at = ?, secret = ? where id = ?");
	ubyte[] bytes = [0x4D, 0x49, 0x4C, 0x4B];
	foreach(i; 0..100)
		conn.execute(upd, i, Clock.currTime, MySQLBinary(bytes), i);


	// passing variable or large number of arguments
	string[] names;
	string[] emails;
	int[] ids = [1, 1, 3, 5, 8, 13];
	conn.execute("select name from users where id in " ~ ids.placeholders, ids, (MySQLRow row) {
		writeln(row.name.peek!(char[])); // peek() avoids allocation - cannot use result outside delegate
		names ~= row.name.get!string;    // get() duplicates - safe to use result outside delegate
		emails ~= row.email.get!string;
	});


	// another query example
	conn.execute("select id, name, email from users where id > ?", 13, (size_t index /*optional*/, MySQLHeader header /*optional*/, MySQLRow row) {
		writeln(header[0].name, ": ", row.id.get!int);
		return (index < 5); // optionally return false to discard remaining results
	});


	// structured row
	conn.execute("select name, email from users where length(name) > ?", 5, (MySQLRow row) {
		auto user = row.toStruct!User; // default is strict.yesIgnoreNull - a missing field in the row will throw
		// auto user = row.toStruct!(User, Strict.yes); // missing or null will throw
		// auto user = row.toStruct!(User, Strict.no);  // missing or null will just be ignored
		writeln(user);
	});


	// structured row with nested structs
	struct GeoRef {
		double lat;
		double lng;
	}

	struct Place {
		string name;
		GeoRef location;
	}

	conn.execute("select name, lat as `location.lat`, lng as `location.lng` from places", (MySQLRow row) {
		auto place = row.toStruct!Place;
		writeln(place.location);
	});


	// structured row annotations
	struct PlaceFull {
		uint id;
		string name;
		@optional string thumbnail;    // ok to be null or missing
		@optional GeoRef location;     // nested fields ok to be null or missing
		@optional @as("contact_person") string contact; // optional, and sourced from field contact_person instead

		@ignore File tumbnail;    // completely ignored
	}

	conn.execute("select id, name, thumbnail, lat as `location.lat`, lng as `location.lng`, contact_person from places", (MySQLRow row) {
		auto place = row.toStruct!PlaceFull;
		writeln(place.location);
	});


	// automated struct member uncamelcase
	struct PlaceOwner {
	@snakeCase:
		uint placeID;            // matches placeID and place_id
		uint locationId;         // matches locationId and location_id
		string ownerFirstName;   // matches ownerFirstName and owner_first_name
		string ownerLastName;    // matches ownerLastName and owner_last_name
		string feedURL;          // matches feedURL and feed_url
	}

	conn.close();
}
```