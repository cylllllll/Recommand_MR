package com;

import java.io.IOException;
import java.net.URISyntaxException;

public class Step6 {

    public static void main(String[] args) {
        int result1 = -1;
        int result2 = -1;
        int result3 = -1;
        int result4 = -1;
        int result5 = -1;
        try {
            result1 = new Step1().run();
        } catch (Exception e) {
            result1 = -1;
        }
        if (result1 == 1) {
            System.out.println("Step1 run success");
            try {
                result2 = new Step2().run();
            } catch (ClassNotFoundException | IOException | InterruptedException | URISyntaxException e) {
                result2 = -1;
            }
        } else {
            System.out.println("Step1 run failed");
        }

        if (result2 == 1) {
            System.out.println("Step2 run success");
            try {
                result3 = new Step3().run();
            } catch (Exception e) {
                result3 = -1;
            }
        } else {
            System.out.println("Step2 run failed");
        }


        if (result3 == 1) {
            System.out.println("Step3 run success");
            try {
                result4 = new Step4().run();
            } catch (Exception e) {
                result4 = -1;
            }
        } else {
            System.out.println("Step3 run failed");
        }

        if (result4 == 1) {
            System.out.println("Step4 run success");
            try {
                result5 = new Step5().run();
            } catch (Exception e) {
                result5 = -1;
            }
        } else {
            System.out.println("Step4 run failed");
        }

        if (result5 == 1) {
            System.out.println("Step5 run success");
            System.out.println("job finished ");
        } else {
            System.out.println("Step5 run failed");
        }

    }

}