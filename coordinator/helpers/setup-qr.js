ObjC.import('Foundation');
ObjC.import('CoreImage');

function run() {
    const text = $.NSString.alloc.initWithDataEncoding(
        $.NSFileHandle.fileHandleWithStandardInput.readDataToEndOfFile, $.NSUTF8StringEncoding);
    const filter = $.CIFilter.filterWithName('CIQRCodeGenerator');
    filter.setValueForKey(text.dataUsingEncoding($.NSUTF8StringEncoding), 'inputMessage');
    filter.setValueForKey('M', 'inputCorrectionLevel');
    const image = filter.outputImage;
    const size = image.extent.size.width;
    const data = $.NSMutableData.dataWithLength(size * size * 4);
    const context = $.CIContext.contextWithOptions($({}));
    context.renderToBitmapRowBytesBoundsFormatColorSpace(image, data.mutableBytes, size * 4, image.extent,
        $.kCIFormatRGBA8, $.CIColor.colorWithRedGreenBlue(0, 0, 0).colorSpace);
    return JSON.stringify({size: size, rgba: ObjC.unwrap(data.base64EncodedStringWithOptions(0))});
}
