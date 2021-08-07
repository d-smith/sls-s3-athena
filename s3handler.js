const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const readline = require('readline');

const through2 = require('through2');
const stream = require('stream');
let {ParquetSchema, ParquetTransformer} = require('parquetjs');

const schema = new ParquetSchema({
    EventId: { type: "UTF8"},
    EventTime: { type: "UTF8"},
    EventType: { type: "UTF8"},
    EventSubject: { type: "UTF8"},
    EventPayload: { type: "UTF8"}
});

const uploadStream = ({Bucket,Key}) => {
    const s3 = new AWS.S3();
    const pass = new stream.PassThrough();
    return {
        writeStream: pass,
        promise: s3.upload({Bucket, Key, Body: pass}).promise()
    };
};


let processObject = async(bucket, sourceKey) => {

    //Form destination key
    let destKey = sourceKey.replace('pubrecord/', 'indexed/');
    console.log(`write to ${destKey}`);

    //Output pipeline set up
    var objectStream = through2.obj(function (chunk, encoding, callback){
        this.push(chunk);
        callback();
    });

    const { writeStream, writePromise } = uploadStream({Bucket: bucket, Key: destKey});

    objectStream
        .pipe(new ParquetTransformer(schema))
        .pipe(writeStream);

    // Set up source stream read
    const params = {
        Bucket: bucket,
        Key: sourceKey
    };

    const s3stream = s3.getObject(params).createReadStream();
    const rl = readline.createInterface({
        input: s3stream,
        terminal: false
    });

    for await(const line of rl) {
        console.log(`read line ${line}`);

        let parsed = JSON.parse(line);
        objectStream.write({
            EventId: parsed.id,
            EventTime: parsed.time,
            EventType: parsed.type,
            EventSubject: parsed.subject,
            EventPayload: line
        });
    }

    objectStream.push(null); //Close the stream

    await writePromise;

};


let handler = async(event) => {
    console.log(JSON.stringify(event));
    let recs = event.Records;
    for(rec of recs) {
        let sourceKey = rec.s3.object.key;
        console.log(`event source object ${sourceKey} in ${rec.s3.bucket.name}`);
        if(sourceKey.startsWith('pubrecord/')) {
            console.log('processing object...');
            await processObject(
                rec.s3.bucket.name,
                decodeURIComponent(sourceKey)
            );
        } else {
            console.log(`ignoring bucket event for ${sourceKey}`);
        }
    }
}


module.exports = {
    handler
};