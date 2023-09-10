use std::env;
use std::thread;

/*
* Print the number of partitions and the size of each partition
*/
fn print_partition_info(vs: &Vec<Vec<usize>>){
    println!("Number of partitions = {}", vs.len());
    for i in 0..vs.len(){
        println!("\tsize of partition {} = {}", i, vs[i].len());
    }
}

/*
* Create a vector with integers from 0 to num_elements -1
*/
fn generate_data(num_elements: usize) -> Vec<usize>{
    let mut v : Vec<usize> = Vec::new();
    for i in 0..num_elements {
        v.push(i);
    }
    return v;
}

/*
* Partition the data in the vector v into 2 vectors
*/
fn partition_data_in_two(v: &Vec<usize>) -> Vec<Vec<usize>>{
    let partition_size = v.len() / 2;
    let mut xs: Vec<Vec<usize>> = Vec::new();

    let mut x1 : Vec<usize> = Vec::new();
    for i in 0..partition_size{
        x1.push(v[i]);
    }
    xs.push(x1);

    let mut x2 : Vec<usize> = Vec::new();
    for i in partition_size..v.len(){
        x2.push(v[i]);
    }
    xs.push(x2);
    xs
}

/*
* Sum up the all the integers in the given vector
*/
fn map_data(v: &Vec<usize>) -> usize{
    let mut sum = 0;
    for i in v{
        sum += i;
    }
    sum
}

/*
* Sum up the all the integers in the given vector
*/
fn reduce_data(v: &Vec<usize>) -> usize{
    let mut sum = 0;
    for i in v{
        sum += i;
    }
    sum
}

/*
* A single threaded map-reduce program
*/
fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        println!("ERROR: Usage {} num_partitions num_elements", args[0]);
        return;
    }
    let num_partitions : usize = args[1].parse().unwrap();
    let num_elements : usize = args[2].parse().unwrap();
    if num_partitions < 1{
      println!("ERROR: num_partitions must be at least 1");
        return;
    }
    if num_elements < num_partitions{
        println!("ERROR: num_elements cannot be smaller than num_partitions");
        return;
    }

    let v = generate_data(num_elements);

    let xs = partition_data_in_two(&v);

    print_partition_info(&xs);

    let mut intermediate_sums : Vec<usize> = Vec::new();

    let clone_of_xs1 = xs[0].clone();
    let clone_of_xs2 = xs[1].clone();

    let thread1 = thread::spawn(move || map_data(&clone_of_xs1));
    let thread2 = thread::spawn(move || map_data(&clone_of_xs2));
    
    intermediate_sums.push(thread1.join().unwrap());
    intermediate_sums.push(thread2.join().unwrap());

    println!("Intermediate sums = {:?}", intermediate_sums);

    let sum = reduce_data(&intermediate_sums);
    println!("Sum = {}", sum);

    let parted_data = partition_data(num_partitions, &v);

    print_partition_info(&parted_data);

    let mut intermediate_sums_new : Vec<usize> = Vec::new();

    for i in parted_data{
        let clone = i.clone();
        let thread = thread::spawn(move || map_data(&clone));
        intermediate_sums_new.push(thread.join().unwrap());
    }

    println!("Intermediate sums = {:?}", intermediate_sums_new);

    let sum = reduce_data(&intermediate_sums_new);

    println!("Sum = {}", sum);

}

fn partition_data(num_partitions: usize, v: &Vec<usize>) -> Vec<Vec<usize>>{

    let partition_size = v.len() / num_partitions;
    let mut partition_size_remainder = v.len() % num_partitions;

    let mut xs_clone: Vec<Vec<usize>> = Vec::new();
    let mut curr_idx = 0;

    for _i in 0..num_partitions {
        let mut x2_clone : Vec<usize> = Vec::new();

        if partition_size_remainder > 0 {
            for _j in 0..partition_size + 1{
                x2_clone.push(v[curr_idx]);
                curr_idx += 1;
            }
            partition_size_remainder -= 1;
        }
        else {
            for _j in 0..partition_size{
                x2_clone.push(v[curr_idx]);
                curr_idx += 1;
            }
        }
        xs_clone.push(x2_clone);
    }
    xs_clone

}