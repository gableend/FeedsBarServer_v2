import Parser from "rss-parser";
export declare const parser: Parser<{
    [key: string]: any;
}, {
    "media:content": any;
} & {
    mediaContent: any;
} & {
    "content:encoded": any;
} & {
    contentEncoded: any;
}>;
/**
 * EXACT same logic as before, just renamed
 */
export declare function findImage(item: any): string | null;
/**
 * EXACT same logic as before, just renamed
 */
export declare function isFatalError(err: any): boolean;
