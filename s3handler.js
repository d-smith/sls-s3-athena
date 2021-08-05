

let handler = async(event) => {
    console.log(JSON.stringify(event));
    let recs = event.Records;
    for(rec of recs) {
        let sourceKey = rec.s3.object.key;
        console.log(`event source object ${sourceKey} in ${rec.s3.bucket.name}`);
        if(sourceKey.startsWith('pubrecord/')) {
            console.log('processing object...');
        } else {
            console.log(`ignoring bucket event for ${sourceKey}`);
        }
    }
}


module.exports = {
    handler
};