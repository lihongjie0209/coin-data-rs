use chrono::Utc;
use coin_data_rs::{futures_parser, parser};
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};

const SPOT: &[&[u8]] = &[
    br#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1720000000000,"s":"BTCUSDT","U":100,"u":110,"b":[["60000.10","1.25"],["59999.90","2.00"]],"a":[["60000.20","0.75"]]}}"#,
    br#"{"stream":"ethusdt@aggTrade","data":{"e":"aggTrade","E":1720000000001,"s":"ETHUSDT","a":12345,"p":"3200.12000000","q":"0.42000000","f":234,"l":236,"T":1720000000000,"m":false,"M":true}}"#,
    br#"{"stream":"!ticker@arr","data":[{"e":"24hrTicker","E":1720000000002,"s":"BTCUSDT","p":"100.1","P":"0.17","w":"59800.0","x":"59900.0","c":"60000.1","Q":"0.1","b":"60000.0","B":"1.2","a":"60000.2","A":"0.7","o":"59900.0","h":"61000.0","l":"58000.0","v":"12345.6","q":"738000000","O":1719913600000,"C":1720000000002,"F":1,"L":200,"n":200},{"e":"24hrTicker","E":1720000000002,"s":"ETHUSDT","p":"20.1","P":"0.63","w":"3180.0","x":"3179.0","c":"3200.1","Q":"2.1","b":"3200.0","B":"4.2","a":"3200.2","A":"3.7","o":"3180.0","h":"3250.0","l":"3100.0","v":"54321.6","q":"172000000","O":1719913600000,"C":1720000000002,"F":1,"L":300,"n":300}]}"#,
];

const FUTURES: &[&[u8]] = &[
    br#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1720000000010,"T":1720000000009,"s":"BTCUSDT","U":1000,"u":1010,"pu":999,"b":[["60000.10","12.25"],["59999.90","20.00"]],"a":[["60000.20","7.75"]]}}"#,
    br#"{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":1720000000011,"a":7654321,"s":"BTCUSDT","p":"60000.12000000","q":"0.42000000","f":123,"l":126,"T":1720000000010,"m":true}}"#,
    br#"{"stream":"!markPrice@arr@1s","data":[{"e":"markPriceUpdate","E":1720000000012,"s":"BTCUSDT","p":"60001.1","P":"60000.8","i":"60002.2","r":"0.00010000","T":1720003600000},{"e":"markPriceUpdate","E":1720000000012,"s":"ETHUSDT","p":"3201.1","P":"3200.8","i":"3202.2","r":"0.00012000","T":1720003600000}]}"#,
    br#"{"stream":"!bookTicker","data":{"e":"bookTicker","u":400900217,"E":1720000000013,"T":1720000000012,"s":"BTCUSDT","b":"60000.10","B":"12.0","a":"60000.20","A":"4.0"}}"#,
    br#"{"stream":"!forceOrder@arr","data":{"e":"forceOrder","E":1720000000014,"o":{"s":"BTCUSDT","S":"SELL","o":"LIMIT","f":"IOC","q":"0.014","p":"60000.0","ap":"59999.0","X":"FILLED","l":"0.014","z":"0.014","T":1720000000013}}}"#,
];

fn parser_replay(criterion: &mut Criterion) {
    let received = Utc::now();
    let mut group = criterion.benchmark_group("binance_parser");
    group.bench_function("spot_batch", |bencher| {
        bencher.iter_batched(
            || Vec::with_capacity(8),
            |mut records| {
                for payload in SPOT {
                    let parsed =
                        parser::parse_into(payload, received, "benchmark_spot", &mut records);
                    assert!(parsed.is_ok(), "fixed spot benchmark payload must parse");
                }
                records
            },
            BatchSize::SmallInput,
        );
    });
    group.bench_function("futures_batch", |bencher| {
        bencher.iter_batched(
            || Vec::with_capacity(8),
            |mut records| {
                for payload in FUTURES {
                    let parsed = futures_parser::parse_into(
                        payload,
                        received,
                        "benchmark_futures",
                        &mut records,
                    );
                    assert!(parsed.is_ok(), "fixed futures benchmark payload must parse");
                }
                records
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

criterion_group!(benches, parser_replay);
criterion_main!(benches);
