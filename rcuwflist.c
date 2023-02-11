/*
 * rcuwflist.c: RCU-based ordered singly-linked list with
 * 		wait-free lookup operations.
 *
 * Copyright (c) 2021 Junchang Wang
 * Copyright (c) 2021 Dunwei Liu.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
 *
 * Based on the following research paper:
 *   - A Lazy Concurrent List-Based Set Algorithm
 *
 */
#define _LGPL_SOURCE
#define _GNU_SOURCE

#include <errno.h>
#include <urcu/uatomic.h>
#include <urcu/pointer.h>
#include "rcuwflist.h"
#include "api.h"

unsigned long get_flag(struct ht_node *htnp)
{
	return (unsigned long)((uintptr_t)htnp & FLAGS_MASK);
}

struct ht_node * get_ptr(struct ht_node *htnp)
{
	return (struct ht_node *)((uintptr_t)htnp & ~FLAGS_MASK);
}

struct ht_node * ptr_flag(struct ht_node *htnp, unsigned long flag)
{
	return (struct ht_node *)
                    (((uintptr_t)htnp & ~FLAGS_MASK) | flag);
}

void set_next_ptr(struct ht_node *htnp, struct ht_node *next)
{
	struct ht_node *old, *new;

	do {
		old = READ_ONCE(htnp->next);
		new = ptr_flag(next, get_flag(old));
	} while (uatomic_cmpxchg(&htnp->next, old, new) != old);
}

int is_removed(struct ht_node *htnp)
{
	return ((uintptr_t)READ_ONCE(htnp->next) & FLAGS_MASK) != 0;
}

int logically_removed(struct ht_node *htnp)
{
	return ((uintptr_t)READ_ONCE(htnp->next) & LOGICALLY_REMOVED) != 0;
}

void set_snapshot(struct wflist_snapshot *ssp,struct ht_node **prev,
		     struct ht_node *cur, struct ht_node *next)
{
	ssp->prev = prev;
	ssp->cur = get_ptr(cur);
	ssp->next = get_ptr(next);
}

int wflist_find_rcu(struct wflist_rcu *list, unsigned long key,
			struct wflist_snapshot *ssp)
{
	struct ht_node *curr;
	struct ht_node *curr_ptr;

	if ((key == MIN_KEY) || (key == MAX_KEY)) {
		dbg_printf("Invalide key value (%lu) is passed to find_rcu.\n", key);
		BUG_ON(1);
	}

	curr = rcu_dereference(list->head);
	curr_ptr = get_ptr(curr);

	while (READ_ONCE(curr_ptr->key) < key) {
		curr = rcu_dereference(curr_ptr->next);
		curr_ptr = get_ptr(curr);
	}

	if ((curr_ptr->key == key) && !is_removed(curr_ptr)) {
		set_snapshot(ssp, NULL, curr_ptr, NULL);
		return 0;
	}
	else
		return -ENOENT;
}

int validate(struct ht_node *prev, struct ht_node *curr)
{
	return !is_removed(prev) && !is_removed(curr) &&
		READ_ONCE(prev->next) == curr;
}

int wflist_insert_rcu(struct wflist_rcu *list, struct ht_node *node)
{
	struct ht_node *prev, *prev_ptr;
	struct ht_node *curr, *curr_ptr;
	int result;

	unsigned long key = READ_ONCE(node->key);
	if ((key == MIN_KEY) || (key == MAX_KEY)) {
		dbg_printf("Invalide key value (%lu) is passed to insert_rcu.\n", key);
		BUG_ON(1);
	}

	while (1) {
		prev = rcu_dereference(list->head);
		prev_ptr = get_ptr(prev);
		curr = rcu_dereference(prev_ptr->next);
		curr_ptr = get_ptr(curr);

		while (READ_ONCE(curr_ptr->key) < READ_ONCE(node->key)) {
			prev = curr;
			prev_ptr = get_ptr(prev);
			curr = rcu_dereference(curr_ptr->next);
			curr_ptr = get_ptr(curr);
		}
		spin_lock(&prev_ptr->lock);
		spin_lock(&curr_ptr->lock);

		if (validate(prev_ptr, curr_ptr)) {
			if (READ_ONCE(curr_ptr->key) == READ_ONCE(node->key)) {
				result = -1;
				goto out;
			}
			else {
				rcu_assign_pointer(node->next, curr_ptr);
				rcu_assign_pointer(prev_ptr->next, node);
				result = 0;
				goto out;
			}
		}

		spin_unlock(&curr_ptr->lock);
		spin_unlock(&prev_ptr->lock);
	}
out:
	spin_unlock(&curr_ptr->lock);
	spin_unlock(&prev_ptr->lock);
	return result;
}

int wflist_delete_rcu(struct wflist_rcu *list, unsigned long key,
			struct wflist_snapshot *ssp, unsigned long flag)
{
	struct ht_node *prev, *prev_ptr;
	struct ht_node *curr, *curr_ptr;
	int result;

	if ((key == MIN_KEY) || (key == MAX_KEY)) {
		dbg_printf("Invalide key value (%lu) is passed to delete_rcu.\n", key);
		BUG_ON(1);
	}

	while (1) {
		prev = rcu_dereference(list->head);
		prev_ptr = get_ptr(prev);
		curr = rcu_dereference(prev_ptr->next);
		curr_ptr = get_ptr(curr);

		while (READ_ONCE(curr_ptr->key) < key) {
			prev = curr;
			prev_ptr = get_ptr(prev);
			curr = rcu_dereference(curr_ptr->next);
			curr_ptr = get_ptr(curr);
		}
		spin_lock(&prev_ptr->lock);
		spin_lock(&curr_ptr->lock);

		if (validate(prev_ptr, curr_ptr)) {
			if (READ_ONCE(curr_ptr->key) != key) {
				result = -1;
				goto out;
			}
			else {
				set_flag(curr_ptr, flag);
				set_next_ptr(prev_ptr, curr_ptr->next);
				if (list->delete_node)
					list->delete_node(curr_ptr);
				result = 0;
				goto out;
			}
		}

		spin_unlock(&curr_ptr->lock);
		spin_unlock(&prev_ptr->lock);
	}
out:
	spin_unlock(&curr_ptr->lock);
	spin_unlock(&prev_ptr->lock);
	set_snapshot(ssp, NULL, curr_ptr, NULL);
	return result;
}
