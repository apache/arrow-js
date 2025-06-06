declare module 'zstd-codec' {
    export const ZstdCodec: {
        run(callback: (zstd: any) => void): void;
    };
}
