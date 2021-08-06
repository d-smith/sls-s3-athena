const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const readline = require('readline');



let processObject = async(bucket, sourceKey) => {
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
        console.log(line);
    }

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