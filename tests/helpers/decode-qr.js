ObjC.import('Foundation');
ObjC.import('CoreImage');

function run(args) {
    const input = JSON.parse(ObjC.unwrap($.NSString.stringWithContentsOfFileEncodingError(args[0], $.NSUTF8StringEncoding, Ref())));
    const data = $.NSData.alloc.initWithBase64EncodedStringOptions(input.rgba, 0);
    const image = $.CIImage.imageWithBitmapDataBytesPerRowSizeFormatColorSpace(data, input.size * 4,
        {width: input.size, height: input.size}, $.kCIFormatRGBA8, $.CIColor.colorWithRedGreenBlue(0, 0, 0).colorSpace);
    const context = $.CIContext.contextWithOptions($({}));
    const detector = $.CIDetector.detectorOfTypeContextOptions($.CIDetectorTypeQRCode, context,
        $({CIDetectorAccuracy: 'CIDetectorAccuracyHigh'}));
    const features = detector.featuresInImage(image);
    const result = [];
    for (let index = 0; index < features.count; index++) {
        result.push(ObjC.unwrap(features.objectAtIndex(index).messageString));
    }
    return JSON.stringify(result);
}
