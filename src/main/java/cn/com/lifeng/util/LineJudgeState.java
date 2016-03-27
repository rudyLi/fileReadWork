package cn.com.lifeng.util;

/**
 * Created by lifeng on 16/3/28.
 */
public class LineJudgeState {
    //起始状态时为最优状态，只要有数据就要写行号相关的动作
    //状态转移 当 行分隔符为 '\r\n',则状态转移有三种状态（0，1，2） 起始状态为0，每次拿当前状态的对比
    //如果满足则状态加一，否则减一，连续两个匹配上则状态到达完美匹配状态2
    private byte[] transfer = CommonConstant.LINE_SEPARATOR;
    private int perfectState = CommonConstant.LINE_SEPARATOR.length;
    private int currentState;

    public LineJudgeState(int initialState) {
        this.currentState = initialState;
    }

    public int getPerfectState() {
        return perfectState;
    }

    public int getCurrentState() {
        return currentState;
    }

    public int doTransfer(byte tmp) {
        if (currentState == perfectState) currentState = 0;
        if (transfer[currentState] == tmp) {
            currentState++;
        } else if (currentState > 0) {
            currentState--;
        }
        return currentState;
    }
}
