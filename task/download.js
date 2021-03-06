
var fs = require('fs'),
    util = require('util'),
    request = require('request'),
    mkdirp = require('mkdirp'),
    pad = require('pad'),
    progress = require('../util/streamProgressBar'),
    logger = require( 'pelias-logger' ).get( 'geonames' );

// use datapath setting from your config file
var settings = require('pelias-config').generate();
var basepath = settings.imports.geonames.datapath;

module.exports = function (filename) {

  var remoteFilePath = util.format( 'http://download.geonames.org/export/dump/%s.zip', filename );
  var localFileName = util.format( '%s/%s.zip', basepath, filename );

  mkdirp( 'data', function( error ){

    if( error ){
      logger.error( error );
      return;
    }
    logger.info( 'downloading datafile from:', remoteFilePath );

    request.get( remoteFilePath )
      .pipe( progress( pad( localFileName, 30 ) ) )
      .pipe( fs.createWriteStream( localFileName ) );
  });

};
