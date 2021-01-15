package FirstDemo;
//冒泡排序、选择排序、插入排序
//快速排序、归并排序、堆排序、
//希尔排序
//计数排序、基数排序、桶排序
/*
* author:minboyin
* time:2021年1月4日
* usage:bubbleSort、selectionSort
* */
public class Demo {
    public static void main(String[] args) {
        Demo demo=new Demo();
        int[] array = new int[]{3, 5, 4, 1, 8, 7, 155};
        int[] result = demo.bubbleSort(array);
        int[] ans = demo.selectionSort(array);
        for (int a : result) {
            System.out.println(a);
        }
    }
    private int[] bubbleSort(int[] array){
        for (int i = array.length - 1; i > 0; i--){
            for (int j = 0; j < i; j++) {
                if (array[j] > array[j + 1]) {
                    int temp = array[j + 1];
                    array[j + 1] = array[j];
                    array[j] = temp;
                }
            }
//        for (int a : array) {
//            System.out.println(a);
        }
        return array;
    }

    private int[] selectionSort(int[] array){
        return array;
    }
}

