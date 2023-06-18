Given a non-negative integer x, return the square root of x rounded down to the nearest integer. The returned integer should be non-negative as well. You must not use any built-in exponent function or operator. 

 Example 1:
Input: x = 4 Output: 2 Explanation: The square root of 4 is 2, so we return 2.
Example 2:

Input: x = 8 Output: 2 Explanation: The square root of 8 is 2.82842..., and since we round it down to the nearest integer, 2 is returned.
Constraints:

0 <= x <= 2^31 - 1



Solution -  

def findsqrt(target):
    start = 0
    end = target
    mid = start + (end-start)//2
    ans = -1

    while(start<end):
        if mid*mid== target:
            return mid
        elif mid*mid > target:
            end = mid -1
        elif mid*mid < target:
            ans = mid
            start = mid + 1
        mid = start + (end-start)//2
    return ans


target=8
print(findsqrt(target))






You are given two non-empty linked lists representing two non-negative integers. The digits are stored in reverse order, and each of their nodes contains a single digit. Add the two numbers and return the sum as a linked list.

You may assume the two numbers do not contain any leading zero, except the number 0 itself.


Example 1:

Input: l1 = [2,4,3], l2 = [5,6,4] Output: [7,0,8] Explanation: 342 + 465 = 807.

Example 2:

Input: l1 = [0], l2 = [0] Output: [0]

Example 3:

Input: l1 = [9,9,9,9,9,9,9], l2 = [9,9,9,9] Output: [8,9,9,9,0,0,0,1]

 

Constraints:

The number of nodes in each linked list is in the range [1, 100].
0 <= Node.val <= 9 It is guaranteed that the list represents a number that does not have leading zeros.


Solution -

class ListNode:
    def __init__(self, val=0, next=None):
        self.val = val
        self.next = next

def addTwoNumbers(l1: ListNode, l2: ListNode) -> ListNode:
    # Head of the new linked list - this is the head of the resultant list
    head = None
    # Reference of head which is null at this point
    temp = None
    # Carry
    carry = 0
    # Loop for the two lists
    while l1 is not None or l2 is not None:
        # At the start of each iteration, we should add carry from the last iteration
        sum_value = carry
        # Since the lengths of the lists may be unequal, we are checking if the
        # current node is null for one of the lists
        if l1 is not None:
            sum_value += l1.val
            l1 = l1.next
        if l2 is not None:
            sum_value += l2.val
            l2 = l2.next
        # At this point, we will add the total sum_value % 10 to the new node
        # in the resultant list
        node = ListNode(sum_value % 10)
        # Carry to be added in the next iteration
        carry = sum_value // 10
        # If this is the first node or head
        if temp is None:
            temp = head = node
        # for any other node
        else:
            temp.next = node
            temp = temp.next
    # After the last iteration, we will check if there is carry left
    # If it's left then we will create a new node and add it
    if carry > 0:
        temp.next = ListNode(carry)
    return head



# Driver code 
# l1 = ListNode(2)
# l1.next = ListNode(4)
# l1.next.next = ListNode(3)

# l2 = ListNode(5)
# l2.next = ListNode(6)
# l2.next.next = ListNode(4)

# result = addTwoNumbers(l1, l2)
# # The result should be [7,0,8]
# while result:
#     print(result.val, end=" ")
#     result = result.next





























