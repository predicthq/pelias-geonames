
var shapefile = require('shapefile-stream'),
    settings = require('pelias-config').generate(),
    through = require('through2'),
    // dbclient = require('pelias-dbclient')({ batchSize: 1 }),
    model = require( 'pelias-model' );

// use datapath setting from your config file
var basepath = settings.imports.quattroshapes.datapath;

// testing
// basepath = '/media/hdd/osm/quattroshapes/simplified';

var import_path = basepath + '/quattroshapes_gazetteer_gp_then_gn.shp';
// console.log(dbclient.client);

function mapper( item, enc, next ){
  var record;
  var import_props= {
    geoname_id: 'gn_id',
    population: 'gn_pop',
    photos:     'photos',
    photos_all: 'photos_all',
    photos_sr:  'photos_sr',
    popularity: 'pop_sr'
  };
  var geoname_id = parseInt(item.properties[ import_props.geoname_id ], 10) || undefined;
  
  if (geoname_id) {
    // console.log(item.properties.gn_name);
    // console.log(i++);
    var population = item.properties[ import_props.population ] || undefined;
    // var photos     = item.properties[ import_props.photos ] || null;
    // var photos_all = parseInt(item.properties[ import_props.photos_all ], 10) || 0;
    var photos_sr  = parseInt(item.properties[ import_props.photos_sr ], 10) || 0;
    var popularity = parseInt(item.properties[ import_props.popularity ], 10) || 0;
    try {
      record = new model.Document( 'geoname', geoname_id );

      try {
        // check if population is already set?
        population = parseInt(population, 10);
        if (population) {
          record.setPopulation( population );
        }
      } catch( err ){}
      
      try {
        if (photos_sr) {
          record.setPhotos( photos_sr );
        }
      } catch( err ){}

      try {
        if (popularity) {
          record.setPopSr( popularity );
        }
      } catch( err ){}

    } catch( e ){
      console.error(
        'Failed to create a Document from:', geoname_id, 'Exception:', e
      );
    }
  } 

  if( record !== undefined ){
    // console.log(record);
    this.push( record );
  }
  next();
}

module.exports = function(){
  shapefile.createReadStream( import_path, { encoding: 'UTF-8' } )
    .pipe( through.obj( mapper ))
    // .pipe( suggester.pipeline )
    .pipe( through.obj( function( item, enc, next ){
      // console.log(item);
      this.push({
        _index: 'pelias',
        _type: item.getType(),
        _id: item.getId(),
        data: item
      });
      next();
    }));
    // .pipe( dbclient );

  // if( !found ){
  //   console.error( 'please select an import...' );
  //   console.error( imports.map( function( i ){
  //     return i.type;
  //   }).join(', '));
  //   process.exit(1);
  // }
};

