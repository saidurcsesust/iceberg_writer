class Solution{
public:
    ListNode *reverseList(ListNode* head)
    {
        if(!head or !head->next) return head;
        ListNode *prev = head;
        ListNode *cur = head->next;
        ListNode *nxt = head->next->next;
        prev->next = nullptr;
        while(cur)
        {
            cur->next = prev;
            prev = cur;
            cur = nxt;
            nxt = nxt->next;
        }

        return cur;

    }
}