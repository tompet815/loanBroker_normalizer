package models;


import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Date;

public class XMLData implements Serializable {

    private String ssn;
    private int creditScore;
    private double loanAmoount;
    private String loanDuration;

    public XMLData(String ssn, int creditScore, double loanAmoount, String loanDuration) {
        this.ssn = ssn;
        this.creditScore = creditScore;
        this.loanAmoount = loanAmoount;
        this.loanDuration = loanDuration;
    }

    public String getSsn() {
        return ssn;
    }

    public int getCreditScore() {
        return creditScore;
    }

    public double getLoanAmoount() {
        return loanAmoount;
    }

    public String getLoanDuration() {
        return loanDuration;
    }

    public void setSsn(String ssn) {
        this.ssn = ssn;
    }

    public void setCreditScore(int creditScore) {
        this.creditScore = creditScore;
    }

    public void setLoanAmoount(double loanAmoount) {
        this.loanAmoount = loanAmoount;
    }

    public void setLoanDuration(String loanDuration) {
        this.loanDuration = loanDuration;
    }

    @Override
    public String toString() {
        return "Data{" + "ssn=" + ssn + ", creditScore=" + creditScore + ", loanAmoount=" + loanAmoount + ", loanDuration=" + loanDuration + '}';
    }

}
