pub fn binary_search_v2(results: &Vec<u32>, finding: u32) -> u32 {
    let mut minimun: usize = 0;
    let mut maximun: usize = results.len();
    let mut result: Option<u32> = Option::None;

    while result.is_none() {
        let value = results[(minimun + maximun) / 2];
        if value == maximun as u32 {
            result = Option::Some((maximun - 1) as u32);
            break;
        }
        if finding > value {
            minimun = value as usize;
            continue;
        }
        if finding < value {
            maximun = value as usize;
            continue;
        }
        result = Option::Some(value)
    }

    return result.unwrap() as u32;
}
