package models;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import javax.xml.bind.annotation.XmlRootElement;
@XmlRootElement
public class Data implements Serializable {

    private String ssn;
    private int creditScore;
    private double loanAmount;
    private int loanDuration;
    public Data() {
    }
    public Data( String ssn, int creditScore, double loanAmount, int loanDuration ) {
        this.ssn = ssn;
        this.creditScore = creditScore;
        this.loanAmount = loanAmount;
        this.loanDuration = loanDuration;
      
    }

    public String getSsn() {
        return ssn;
    }

    public int getCreditScore() {
        return creditScore;
    }

    public double getLoanAmount() {
        return loanAmount;
    }

    public int getLoanDuration() {
        return loanDuration;
    }


    public void setSsn( String ssn ) {
        this.ssn = ssn;
    }

    public void setCreditScore( int creditScore ) {
        this.creditScore = creditScore;
    }

    public void setLoanAmount( double loanAmount ) {
        this.loanAmount = loanAmount;
    }

    public void setLoanDuration( int loanDuration ) {
        this.loanDuration = loanDuration;
    }

    @Override
    public String toString() {
        return "Data{" + "ssn=" + ssn + ", creditScore=" + creditScore + ", loanAmount=" 
                + loanAmount + ", loanDuration=" + loanDuration +  '}';
    }

}
