// A Braid is a Directed Acyclic Graph with the additional restriction that
// parents may not also be non-parent ancestors. (aka "incest")

//use rusqlite::NO_PARAMS; // Why doesn't this exist, the docs say it does.
use rusqlite::{params, Connection, Result};
use std::fmt;
use std::fs;
use std::path::PathBuf;
use std::error::Error;

mod bead;

use bead::{Bead};

#[derive(Debug)]
pub struct BeadNotFound;
#[derive(Debug)]
pub struct BraidError {
    source: BeadNotFound
}
impl Error for BraidError {}
impl fmt::Display for BraidError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Bead not found")
    }
}

pub struct Braid {
    dbconn: rusqlite::Connection,
}

impl Braid {
    pub fn open(datadir: PathBuf) -> Braid {
        let mut sqlitefile = datadir.clone();
        sqlitefile.push("braid.sqlite");
        if datadir.exists() {
            println!("Using existing data directory: {}", datadir.display());
            match rusqlite::Connection::open(sqlitefile.clone()) {
                Ok(c) => return Braid { dbconn: c },
                Err(error) => panic!("Error opening {}: {}", sqlitefile.display(), error),
            };
        } else {
            println!("Setting up new datadir in {}", datadir.display());
            match fs::create_dir(datadir.clone()) {
                Err(error) => panic!("Problem creating datadir: {:?}", error),
                Ok(()) => (),
            }
        }
        let connection = match rusqlite::Connection::open(sqlitefile.clone()) {
            Ok(c) => c,
            Err(error) => panic!("Error opening {}: {}", sqlitefile.display(), error),
        };
        // FIXME implement db versioning

        // We need a separate table for parents since each bead has an array of parents
        // Get all parents of this bead:
        //      "SELECT parent FROM parents WHERE hash=<>"
        let parents_table = "
            CREATE TABLE parents (
                hash BLOB PRIMARY KEY REFERENCES beads(hash),
                parent BLOB REFERENCES beads(hash)
            )";
        // The above tables are authoritative.

        // The below tables are created by analysis, and non-authoritative.

        // They can be recreated by reindex
        // Get all children of this bead:
        //      "SELECT child FROM children WHERE hash=<>"
        let children_table = "
            CREATE TABLE children (
                hash BLOB PRIMARY KEY REFERENCES beads(hash),
                child BLOB REFERENCES beads(hash)
            )";

        // Siblings are beads which cannot be partial ordered with respect to
        // one another
        // Get all siblings of this bead:
        //      "SELECT sibling FROM siblings WHERE hash=<>"
        let siblings_table = "
            CREATE TABLE siblings (
                hash BLOB PRIMARY KEY REFERENCES beads(hash),
                sibling BLOB REFERENCES beads(hash)
            )";
        // A cohort is a set of beads that can be total ordered with respect to all
        // beads that are ancestors and descendants.
        // To get all members of my cohort:
        //      "SELECT member FROM cohorts WHERE height=(
        //          SELECT height FROM cohorts WHERE member=<>
        //      )"
        let cohorts_table = "
            CREATE TABLE cohorts (
                height INTEGER,
                member BLOB PRIMARY KEY REFERENCES beads(hash)
            )";

        // Create Tables
        for table in vec![bead::BEADS_TABLE_SCHEMA, parents_table, children_table,
                siblings_table, cohorts_table].iter() {
            connection.execute(table, ()).unwrap();
        };

        // Insert Genesis bead
        let genesis_insert = "INSERT INTO beads (hash) VALUES (?)";
        let genesishash: [u8;32] = [0;32];
        connection.execute(genesis_insert, params![genesishash]).unwrap();

        return Braid { dbconn: connection } ;

    //        self.t = network.t
    //        self.hash = hash    # a hash that identifies this block
    //        self.parents = parents
    //        self.children = set() # filled in by Braid.make_children()
    //        self.siblings = set() # filled in by Braid.analyze
    //        self.cohort = set() # filled in by Braid.analyze
    //        self.transactions = transactions
    //        self.network = network
    //        self.creator = creator
    //        if creator != -1: # if we're not the genesis block (which has no creator node)
    //            self.difficulty = MAX_HASH/network.nodes[creator].target
    //        else: self.difficulty = 1
    //        self.sibling_difficulty = 0
    //        network.beads[hash] = self # add myself to global list
    //        self.reward = None  # this bead's reward (filled in by Braid.rewards)

        //connection.execute(query).unwrap();
    }

    pub fn tips(&self) -> Vec<Bead> {
        let mut ret: Vec<Bead> = Vec::new();
        // FIXME this may be O(n) in the number of beads
        /*
        self.dbconn.iterate("
                SELECT b.hash FROM beads b
                LEFT JOIN children c ON b.hash = c.hash
                WHERE c.hash IS NULL", |hashes| {
            for hash in hashes.iter() {
            }
            true
        }).unwrap();
        */
        return ret;
    }

    pub fn get_bead(&self, hash:[u8;32]) -> Result<Bead, Box<dyn Error>> {
        let mut query = self.dbconn.prepare("SELECT * FROM beads WHERE hash = (?)")?;
        let bead = query.query_map(&[&hash], |row| {
            Ok(Bead {
                hash: row.get(0)?,
                blockheader: row.get(1)?,
                coinbase: row.get(2)?,
                payout: row.get(3)?,
                metadata: row.get(4)?,
                ucmetadata: row.get(5)?,
                difficulty: row.get(6)?,
            })
        })?;
        return Err(Box::new(BraidError { source: BeadNotFound }));
    }
}
